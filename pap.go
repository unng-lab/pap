package pap

import (
	"pap/internal/cfg"
	"pap/internal/conn"
)

const (
	min  = 16
	max  = 128
	eMax = 1024
)

type pap struct {
	config cfg.Config

	conns   *connections
	queries *Queries

	queryChan      chan *conn.Query
	emptyQueryChan chan *conn.Query
	connReadyChan  chan int

	ps preparedStatements
}
