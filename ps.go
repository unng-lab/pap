package pap

import (
	"strconv"
	"sync"

	"pap/internal/conn"
)

type Query struct {
	SQL         string
	Args        []interface{}
	Description *conn.Description
}

type preparedStatements struct {
	list  map[string]*conn.Description
	mutex sync.RWMutex
}

func (p *pap) checkDescription(query *conn.Query) (*conn.Description, error) {
	p.ps.mutex.RLock()
	desc, ok := p.ps.list[query.SQL]

	if ok {
		p.ps.mutex.RUnlock()
		return desc, nil
	} else {
		p.ps.mutex.RUnlock()
		p.ps.mutex.Lock()
		defer p.ps.mutex.Unlock()
		query.D = &conn.Description{Name: "pap_ps_" + strconv.Itoa(len(p.ps.list))}
		err := p.prepare(query)
		if err != nil {
			return nil, err
		}
		p.ps.list[query.SQL] = query.D
		return query.D, nil
	}

}
