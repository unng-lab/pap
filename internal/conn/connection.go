package conn

import (
	"net"
	"sync"

	"github.com/jackc/pgproto3/v2"

	"pap/internal/cfg"
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
	frontend          *pgproto3.Frontend

	//config *cfg.Config

	status byte // One of connStatus* constants

	bufferingReceive    bool
	bufferingReceiveMux sync.Mutex
	bufferingReceiveMsg pgproto3.BackendMessage
	bufferingReceiveErr error

	peekedMsg pgproto3.BackendMessage

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
	c.sufBuf = (&pgproto3.Describe{ObjectType: 'P'}).Encode(c.sufBuf)
	c.sufBuf = (&pgproto3.Execute{}).Encode(c.sufBuf)
	c.sufBuf = (&pgproto3.Sync{}).Encode(c.sufBuf)

	var cmd Command

	for {
		cmd = <-commandChan
		switch cmd.CommandType {
		case CommandQuery:
			c.ExecParams(
				cmd.Query,
			)
			c.ready()
			cmd.Query.ready()
		case CommandPrepare:
			c.prepare(
				cmd.Query,
			)
			c.ready()
			cmd.Query.ready()
		case CommandPrepareAsync:
			c.prepareAsync(
				cmd.Query,
			)
			c.ready()
			cmd.Query.Close()
		case CommandPreparedQuery:
			c.ExecPrepared(
				cmd.Query,
			)
			c.ready()
			cmd.Query.ready()
		case CommandFuncCache:
			c.ExecParams(
				cmd.Query,
			)
			c.ready()
			cmd.Query.ready()
		case CommandConnect:
			err := c.connect(cmd.Body.(cfg.Config), &cfg.FallbackConfig{})
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
	c.wBuf = (&pgproto3.Parse{
		Query:         q.SQL,
		ParameterOIDs: q.D.paramOIDs,
	}).Encode(c.wBuf)
	c.wBuf = (&pgproto3.Bind{
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
		case *pgproto3.RowDescription:
			q.D.FieldDescriptions = msg.Fields
		case *pgproto3.EmptyQueryResponse:
			q.R.concludeCommand(nil, nil)
		case *pgproto3.DataRow:
			q.R.rowValues = append(q.R.rowValues, msg.Values...)
		case *pgproto3.ErrorResponse:
			q.R.concludeCommand(nil, ErrorResponseToPgError(msg))
		case *pgproto3.CommandComplete:
			q.R.concludeCommand(msg.CommandTag, nil)
		case *pgproto3.ReadyForQuery:
			q.R.commandConcluded = true
			// TODO CHECK DOCS
			//case *pgproto3.ParseComplete:
			//case *pgproto3.BindComplete:
			//
			//default:
			//	panic("check")
		}
	}
}

func (c *connection) prepare(q *Query) {

	c.wBuf = (&pgproto3.Parse{Name: q.D.Name, Query: q.SQL, ParameterOIDs: q.D.paramOIDs}).Encode(c.wBuf)
	c.wBuf = (&pgproto3.Describe{ObjectType: 'S', Name: q.D.Name}).Encode(c.wBuf)
	c.wBuf = (&pgproto3.Sync{}).Encode(c.wBuf)

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
		case *pgproto3.ParameterDescription:
			q.D.paramOIDs = append(q.D.paramOIDs, msg.ParameterOIDs...)
		case *pgproto3.RowDescription:
			// TODO Name cap 8000 try to reduce mb
			q.D.FieldDescriptions = append(q.D.FieldDescriptions, msg.Fields...)
		case *pgproto3.ErrorResponse:
			parseErr = ErrorResponseToPgError(msg)
		case *pgproto3.ReadyForQuery:
			q.R.commandConcluded = true
			// TODO CHECK DOCS
			//case *pgproto3.ParseComplete:
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

	c.wBuf = (&pgproto3.Parse{Name: q.D.Name, Query: q.SQL, ParameterOIDs: q.D.paramOIDs}).Encode(c.wBuf)
	c.wBuf = (&pgproto3.Describe{ObjectType: 'S', Name: q.D.Name}).Encode(c.wBuf)
	c.wBuf = (&pgproto3.Sync{}).Encode(c.wBuf)

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
		case *pgproto3.ParameterDescription:
			//q.D.paramOIDs = append(q.D.paramOIDs, msg.ParameterOIDs...)
		case *pgproto3.RowDescription:
			// TODO Name cap 8000 try to reduce mb
			//q.D.FieldDescriptions = append(q.D.FieldDescriptions, msg.Fields...)
		case *pgproto3.ErrorResponse:
			parseErr = ErrorResponseToPgError(msg)
		case *pgproto3.ReadyForQuery:
			q.R.commandConcluded = true
			// TODO CHECK DOCS
			//case *pgproto3.ParseComplete:
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
	c.wBuf = (&pgproto3.Bind{
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
		case *pgproto3.RowDescription:
			q.D.FieldDescriptions = msg.Fields
		case *pgproto3.EmptyQueryResponse:
			q.R.concludeCommand(nil, nil)
		case *pgproto3.DataRow:
			q.R.rowValues = append(q.R.rowValues, msg.Values...)
		case *pgproto3.ErrorResponse:
			q.R.concludeCommand(nil, ErrorResponseToPgError(msg))
		case *pgproto3.CommandComplete:
			q.R.concludeCommand(msg.CommandTag, nil)
		case *pgproto3.ReadyForQuery:
			q.R.commandConcluded = true
			// TODO CHECK DOCS
			//case *pgproto3.ParseComplete:
			//case *pgproto3.BindComplete:
			//
			//default:
			//	panic("check")
		}
	}
}
