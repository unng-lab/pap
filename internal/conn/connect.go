/*
 * Copyright (c) 2021-2022 UNNG Lab.
 */

package conn

import (
	"crypto/md5"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"strconv"

	"pap/internal/cfg"
	"pap/internal/pgproto"
)

func (c *connection) connect(config *cfg.Config, fallbackConfig *cfg.FallbackConfig) error {
	c.cleanupDone = make(chan struct{})
	config.RuntimeParams["application_name"] += strconv.Itoa(c.number)
	var err error
	network, address := cfg.NetworkAddress(config.Host, config.Port)
	conn, err := config.DialFunc(network, address)
	c.conn = conn
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			err = &errTimeout{err: err}
		}
		return &connectError{config: config, msg: "dial error", err: err}
	}

	c.parameterStatuses = make(map[string]string)

	if fallbackConfig.TLSConfig != nil {
		if err := c.startTLS(fallbackConfig.TLSConfig); err != nil {
			c.conn.Close()
			return &connectError{config: config, msg: "tls error", err: err}
		}
	}

	c.status = statusConnecting

	c.frontend = config.BuildFrontend(conn, conn)

	startupMsg := pgproto.StartupMessage{
		ProtocolVersion: pgproto.ProtocolVersionNumber,
		Parameters:      make(map[string]string),
	}

	// Copy default run-time params
	for k, v := range config.RuntimeParams {
		startupMsg.Parameters[k] = v
	}

	startupMsg.Parameters["user"] = config.User
	if config.Database != "" {
		startupMsg.Parameters["database"] = config.Database
	}

	if _, err := c.conn.Write(startupMsg.Encode(c.wBuf)); err != nil {
		c.conn.Close()
		return &connectError{config: config, msg: "failed to write startup message", err: err}
	}
	for {
		msg, err := c.receiveMessage()
		if err != nil {
			c.conn.Close()
			if err, ok := err.(*PgError); ok {
				return err
			}
			return &connectError{config: config, msg: "failed to receive message", err: err}
		}

		switch msg := msg.(type) {
		case *pgproto.BackendKeyData:
			c.pid = msg.ProcessID
			c.secretKey = msg.SecretKey

		case *pgproto.AuthenticationOk:
		case *pgproto.AuthenticationCleartextPassword:
			err = c.txPasswordMessage(c.wBuf, config.Password)
			if err != nil {
				c.conn.Close()
				return &connectError{config: config, msg: "failed to write password message", err: err}
			}
		case *pgproto.AuthenticationMD5Password:
			digestedPassword := "md5" + hexMD5(hexMD5(config.Password+config.User)+string(msg.Salt[:]))
			err = c.txPasswordMessage(c.wBuf, digestedPassword)
			if err != nil {
				c.conn.Close()
				return &connectError{config: config, msg: "failed to write password message", err: err}
			}
		case *pgproto.AuthenticationSASL:
			err = c.scramAuth(msg.AuthMechanisms, config)
			if err != nil {
				c.conn.Close()
				return &connectError{config: config, msg: "failed SASL auth", err: err}
			}

		case *pgproto.ReadyForQuery:
			c.status = statusIdle
			//if config.ValidateConnect != nil {
			//	// ValidateConnect may execute commands that cause the context to be watched again. Unwatch first to avoid
			//	// the watch already in progress panic. This is that last thing done by this method so there is no need to
			//	// restart the watch after ValidateConnect returns.
			//	//
			//	// See https://github.com/jackc/c/issues/40.
			//	c.contextWatcher.Unwatch()
			//
			//	err := config.ValidateConnect(ctx, c)
			//	if err != nil {
			//		c.conn.Close()
			//		return nil, &connectError{config: config, msg: "ValidateConnect failed", err: err}
			//	}
			//}
			return nil
		case *pgproto.ParameterStatus:
			// handled by ReceiveMessage
		case *pgproto.ErrorResponse:
			c.conn.Close()
			return ErrorResponseToPgError(msg)
		default:
			c.conn.Close()
			return &connectError{config: config, msg: "received unexpected message", err: err}
		}
	}
}

func (c *connection) startTLS(tlsConfig *tls.Config) (err error) {
	err = binary.Write(c.conn, binary.BigEndian, []int32{8, 80877103})
	if err != nil {
		return
	}

	response := make([]byte, 1)
	if _, err = io.ReadFull(c.conn, response); err != nil {
		return
	}

	if response[0] != 'S' {
		return errors.New("server refused TLS connection")
	}

	c.conn = tls.Client(c.conn, tlsConfig)

	return nil
}

func (c *connection) txPasswordMessage(buf []byte, password string) (err error) {
	msg := &pgproto.PasswordMessage{Password: password}
	_, err = c.conn.Write(msg.Encode(buf))
	return err
}

func hexMD5(s string) string {
	hash := md5.New()
	io.WriteString(hash, s)
	return hex.EncodeToString(hash.Sum(nil))
}
