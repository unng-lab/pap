/*
 * Copyright (c) 2021-2022 UNNG Lab.
 */

package pap

import (
	"sync"
	"time"

	"pap/internal/pgtype"

	"pap/internal/conn"
)

// Queries is a slice of preallocated queries
type Queries struct {
	mutex          sync.Mutex
	ticker         *time.Ticker
	emptyQueryChan chan *conn.Query
	list           []*conn.Query
}

func NewQueries(count int, emptyQueryChan chan *conn.Query) *Queries {
	var q Queries
	cInfo := pgtype.NewConnInfo()
	q.list = make([]*conn.Query, count)
	for i := range q.list {
		q.list[i] = conn.NewQuery(cInfo, emptyQueryChan)
	}
	q.ticker = time.NewTicker(time.Duration(conn.MaxResultSaveDurationInNanoseconds))
	go q.startQueries()
	return &q
}

func (q *Queries) startQueries() {
	for {
		<-q.ticker.C
		if len(q.emptyQueryChan) < len(q.list)/4 {
			for i := range q.list {
				if !q.list[i].Actual() {
					q.list[i].Return()
				}
			}
		}
	}
}
