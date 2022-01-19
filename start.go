package pap

import (
	"sync"

	"pap/internal/cfg"
	"pap/internal/conn"
)

func Start(connString string) (*pap, error) {
	var config cfg.Config
	err := config.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	var p = &pap{
		config: config,
	}

	conns := make([]connection, max)
	emptyQueryChan := make(chan *conn.Query, max)
	p.emptyQueryChan = emptyQueryChan

	queries := NewQueries(max, emptyQueryChan)
	p.queries = queries

	for i := range queries.list {
		emptyQueryChan <- queries.list[i]
	}

	qChan := make(chan *conn.Query, max)
	p.queryChan = qChan
	commandChans := make([]chan conn.Command, max)

	connReadyChan := make(chan int, max)
	p.connReadyChan = connReadyChan

	for i := range conns {
		cChan := make(chan conn.Command, min)
		conns[i].commandChan = cChan
		conn.Start(i, cChan, connReadyChan)
		commandChans[i] = cChan
	}
	p.conns = &connections{list: conns}

	p.ps = preparedStatements{
		list:  make(map[string]*conn.Description, max),
		mutex: sync.RWMutex{},
	}

	p.connect(10)

	go p.start(
		qChan,
		connReadyChan,
	)

	return p, nil
}

func (p *pap) start(
	qChan chan *conn.Query,
	connReadyChan chan int,
) {
	var q *conn.Query
	var cr int
	for {
		q = <-qChan
		cr = <-connReadyChan
		p.conns.list[cr].commandChan <- conn.Command{
			CommandType: conn.CommandPreparedQuery,
			Query:       q,
		}
	}
}
