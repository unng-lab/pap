/*
 * Copyright (c) 2021-2022 UNNG Lab.
 */

package conn

import (
	"net"
	"sync"

	"pap/internal/cfg"
	"pap/internal/pgproto"
)

type Command struct {
	CommandType byte
	Query       *Query
	Body        interface{}
}

type connection struct {
	conn              net.Conn          // the underlying TCP or unix domain socket connection
	pid               uint32            // backend pid
	secretKey         uint32            // key to use to send a cancel query message to the server
	parameterStatuses map[string]string // parameters that have been reported by the server
	txStatus          byte
	frontend          *pgproto.Frontend

	//config *cfg.Config

	status byte // One of connStatus* constants

	bufferingReceive    bool
	bufferingReceiveMux sync.Mutex
	bufferingReceiveMsg pgproto.BackendMessage
	bufferingReceiveErr error

	peekedMsg pgproto.BackendMessage

	cleanupDone chan struct{}

	//new
	number        int
	commandChan   chan Command
	connReadyChan chan int

	//buffers
	wBuf   []byte
	sufBuf []byte
}

func Start(
	number int,
	commandChan chan Command,
	connReadyChan chan int,
) {
	go start(number, commandChan, connReadyChan)
}

func start(
	number int,
	commandChan chan Command,
	connReadyChan chan int,
) {
	var c = connection{
		number:        number,
		commandChan:   commandChan,
		connReadyChan: connReadyChan,
		wBuf:          make([]byte, 0, wbufLen),
	}
	c.sufBuf = make([]byte, 0, 22)
	c.sufBuf = (&pgproto.Describe{ObjectType: 'P'}).Encode(c.sufBuf)
	c.sufBuf = (&pgproto.Execute{}).Encode(c.sufBuf)
	c.sufBuf = (&pgproto.Sync{}).Encode(c.sufBuf)

	var cmd Command

	for {
		cmd = <-commandChan
		switch cmd.CommandType {
		case CommandQuery:
			c.ready()
			c.ExecParams(
				cmd.Query,
			)
			cmd.Query.ready()
		case CommandPrepare:
			c.ready()
			c.prepare(
				cmd.Query,
			)
			cmd.Query.ready()
		case CommandPrepareAsync:
			c.ready()
			c.prepareAsync(
				cmd.Query,
			)
			cmd.Query.Close()
		case CommandPreparedQuery:
			c.ready()
			c.ExecPrepared(
				cmd.Query,
			)
			cmd.Query.ready()
		case CommandFuncCache:
			c.ExecParams(
				cmd.Query,
			)
			c.ready()
			cmd.Query.ready()
		case CommandConnect:
			err := c.connect(cmd.Body.(*cfg.Config), &cfg.FallbackConfig{})
			if err != nil {
				// TODO think about sync
				panic(err)
			}
			c.ready()
		}
	}
}

func (c *connection) ready() {
	c.wBuf = c.wBuf[:0]
	if len(c.commandChan) == 0 {
		c.connReadyChan <- c.number
	}
}

func (c *connection) ExecParams(
	q *Query,
) {
	c.wBuf = (&pgproto.Parse{
		Query:         q.SQL,
		ParameterOIDs: q.D.paramOIDs,
	}).Encode(c.wBuf)
	c.wBuf = (&pgproto.Bind{
		ParameterFormatCodes: q.paramFormats,
		Parameters:           q.paramValues,
		ResultFormatCodes:    q.D.resultFormats,
	}).Encode(c.wBuf)

	n, err := c.conn.Write(append(c.wBuf, c.sufBuf...))

	if err != nil {
		// TODO close connection
		c.status = statusClosed
		q.R.concludeCommand(nil, &writeError{err: err, safeToRetry: n == 0})
		return
	}

	for !q.R.commandConcluded {
		msg, err := c.receiveMessage()

		if err != nil {
			q.R.concludeCommand(nil, err)
			q.R.err = err
			return
		}
		switch msg := msg.(type) {
		case *pgproto.RowDescription:
			q.D.FieldDescriptions = msg.Fields
		case *pgproto.EmptyQueryResponse:
			q.R.concludeCommand(nil, nil)
		case *pgproto.DataRow:
			q.R.rowValues = append(q.R.rowValues, msg.Values...)
		case *pgproto.ErrorResponse:
			q.R.concludeCommand(nil, ErrorResponseToPgError(msg))
		case *pgproto.CommandComplete:
			q.R.concludeCommand(msg.CommandTag, nil)
		case *pgproto.ReadyForQuery:
			q.R.commandConcluded = true
			// TODO CHECK DOCS
			//case *pgproto.ParseComplete:
			//case *pgproto.BindComplete:
			//
			//default:
			//	panic("check")
		}
	}
}

func (c *connection) prepare(q *Query) {

	c.wBuf = (&pgproto.Parse{Name: q.D.Name, Query: q.SQL, ParameterOIDs: q.D.paramOIDs}).Encode(c.wBuf)
	c.wBuf = (&pgproto.Describe{ObjectType: 'S', Name: q.D.Name}).Encode(c.wBuf)
	c.wBuf = (&pgproto.Sync{}).Encode(c.wBuf)

	n, err := c.conn.Write(c.wBuf)
	if err != nil {
		// TODO close connection
		c.status = statusClosed
		q.R.concludeCommand(nil, &writeError{err: err, safeToRetry: n == 0})
		return
	}

	var parseErr error

	for !q.R.commandConcluded {
		msg, err := c.receiveMessage()
		if err != nil {
			// TODO close connection
			c.status = statusClosed
			q.R.concludeCommand(nil, &writeError{err: err, safeToRetry: n == 0})
			return
		}

		switch msg := msg.(type) {
		case *pgproto.ParameterDescription:
			q.D.paramOIDs = append(q.D.paramOIDs, msg.ParameterOIDs...)
		case *pgproto.RowDescription:
			// TODO Name cap 8000 try to reduce mb
			q.D.FieldDescriptions = append(q.D.FieldDescriptions, msg.Fields...)
		case *pgproto.ErrorResponse:
			parseErr = ErrorResponseToPgError(msg)
		case *pgproto.ReadyForQuery:
			q.R.commandConcluded = true
			// TODO CHECK DOCS
			//case *pgproto.ParseComplete:
			//
			//default:
			//	panic("check")
		}
	}

	if parseErr != nil {
		q.R.err = parseErr
		return
	}
}

func (c *connection) prepareAsync(q *Query) {

	c.wBuf = (&pgproto.Parse{Name: q.D.Name, Query: q.SQL, ParameterOIDs: q.D.paramOIDs}).Encode(c.wBuf)
	c.wBuf = (&pgproto.Describe{ObjectType: 'S', Name: q.D.Name}).Encode(c.wBuf)
	c.wBuf = (&pgproto.Sync{}).Encode(c.wBuf)

	n, err := c.conn.Write(c.wBuf)
	if err != nil {
		// TODO close connection
		c.status = statusClosed
		q.R.concludeCommand(nil, &writeError{err: err, safeToRetry: n == 0})
		return
	}

	var parseErr error

	for !q.R.commandConcluded {
		msg, err := c.receiveMessage()
		if err != nil {
			// TODO close connection
			c.status = statusClosed
			q.R.concludeCommand(nil, &writeError{err: err, safeToRetry: n == 0})
			return
		}

		switch msg := msg.(type) {
		case *pgproto.ParameterDescription:
			//q.D.paramOIDs = append(q.D.paramOIDs, msg.ParameterOIDs...)
		case *pgproto.RowDescription:
			// TODO Name cap 8000 try to reduce mb
			//q.D.FieldDescriptions = append(q.D.FieldDescriptions, msg.Fields...)
		case *pgproto.ErrorResponse:
			parseErr = ErrorResponseToPgError(msg)
		case *pgproto.ReadyForQuery:
			q.R.commandConcluded = true
			// TODO CHECK DOCS
			//case *pgproto.ParseComplete:
			//
			//default:
			//	panic("check")
		}
	}

	if parseErr != nil {
		q.R.err = parseErr
		return
	}
}

func (c *connection) ExecPrepared(q *Query) {
	c.wBuf = (&pgproto.Bind{
		PreparedStatement:    q.D.Name,
		ParameterFormatCodes: q.paramFormats,
		Parameters:           q.paramValues,
		ResultFormatCodes:    q.D.resultFormats,
	}).Encode(c.wBuf)

	n, err := c.conn.Write(append(c.wBuf, c.sufBuf...))

	if err != nil {
		// TODO close connection
		c.status = statusClosed
		q.R.concludeCommand(nil, &writeError{err: err, safeToRetry: n == 0})
		return
	}

	for !q.R.commandConcluded {
		msg, err := c.receiveMessage()

		if err != nil {
			q.R.concludeCommand(nil, err)
			q.R.err = err
			return
		}
		switch msg := msg.(type) {
		case *pgproto.RowDescription:
			q.D.FieldDescriptions = msg.Fields
		case *pgproto.EmptyQueryResponse:
			q.R.concludeCommand(nil, nil)
		case *pgproto.DataRow:
			q.R.rowValues = append(q.R.rowValues, msg.Values...)
		case *pgproto.ErrorResponse:
			q.R.concludeCommand(nil, ErrorResponseToPgError(msg))
		case *pgproto.CommandComplete:
			q.R.concludeCommand(msg.CommandTag, nil)
		case *pgproto.ReadyForQuery:
			q.R.commandConcluded = true
			// TODO CHECK DOCS
			//case *pgproto.ParseComplete:
			//case *pgproto.BindComplete:
			//
			//default:
			//	panic("check")
		}
	}
}
