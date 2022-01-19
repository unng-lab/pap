package conn

import (
	"errors"
	"net"

	"github.com/jackc/pgproto3/v2"
)

// peekMessage peeks at the next message without setting up context cancellation.
func (c *connection) peekMessage() (pgproto3.BackendMessage, error) {
	if c.peekedMsg != nil {
		return c.peekedMsg, nil
	}

	var msg pgproto3.BackendMessage
	var err error
	if c.bufferingReceive {
		c.bufferingReceiveMux.Lock()
		msg = c.bufferingReceiveMsg
		err = c.bufferingReceiveErr
		c.bufferingReceiveMux.Unlock()
		c.bufferingReceive = false

		// If a timeout error happened in the background try the read again.
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			msg, err = c.frontend.Receive()
		}
	} else {
		msg, err = c.frontend.Receive()
	}

	if err != nil {
		// Close on anything other than timeout error - everything else is fatal
		var netErr net.Error
		isNetErr := errors.As(err, &netErr)
		if !(isNetErr && netErr.Timeout()) {
			// TODO close
			c.status = statusClosed
		}

		return nil, err
	}

	c.peekedMsg = msg
	return msg, nil
}

// receiveMessage receives a message without setting up context cancellation
func (c *connection) receiveMessage() (pgproto3.BackendMessage, error) {
	msg, err := c.peekMessage()
	if err != nil {
		// Close on anything other than timeout error - everything else is fatal
		var netErr net.Error
		isNetErr := errors.As(err, &netErr)
		if !(isNetErr && netErr.Timeout()) {
			// TODO close
			c.status = statusClosed
		}

		return nil, err
	}
	c.peekedMsg = nil

	switch msg := msg.(type) {
	case *pgproto3.ReadyForQuery:
		c.txStatus = msg.TxStatus
	case *pgproto3.ParameterStatus:
		c.parameterStatuses[msg.Name] = msg.Value
	case *pgproto3.ErrorResponse:
		if msg.Severity == "FATAL" {
			c.status = statusClosed
			c.conn.Close() // Ignore error as the connection is already broken and there is already an error to return.
			close(c.cleanupDone)
			return nil, ErrorResponseToPgError(msg)
		}
	case *pgproto3.NoticeResponse:
		//if c.config.OnNotice != nil {
		//	c.config.OnNotice(c, noticeResponseToNotice(msg))
		//}
	case *pgproto3.NotificationResponse:
		//if c.config.OnNotification != nil {
		//	c.config.OnNotification(c, &Notification{PID: msg.PID, Channel: msg.Channel, Payload: msg.Payload})
		//}
	}

	return msg, nil
}

// Conn returns the underlying net.Conn.
func (c *connection) Conn() net.Conn {
	return c.conn
}

// PID returns the backend PID.
func (c *connection) PID() uint32 {
	return c.pid
}
