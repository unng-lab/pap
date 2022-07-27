/*
 * Copyright (c) 2021-2022 UNNG Lab.
 */

package pap

import (
	"errors"

	"pap/internal/conn"
)

var ErrResultNotActual = errors.New("result not actual")
var ErrArgsLimit = errors.New("args limit")

func (p *Pap) QueryAsync(sql string, args ...interface{}) conn.ResultFunc {
	if !checkArgs(len(args)) {
		return func(dest interface{}) error {
			return ErrArgsLimit
		}
	}
	eq := <-p.emptyQueryChan
	eq.Mutex.Lock()
	err := eq.Start(
		sql,
		args...,
	)
	if err != nil {
		return func(dest interface{}) error {
			return err
		}
	}

	eq.D, err = p.checkDescription(eq)

	if err != nil {
		return func(dest interface{}) error {
			return err
		}
	}

	for i := range eq.Args {
		err = eq.AppendParam(i)
		if err != nil {
			return func(dest interface{}) error {
				return err
			}
		}
	}

	p.queryChan <- eq
	return func(dest interface{}) error {
		eq.Mutex.Lock()
		defer eq.Close()
		if !eq.Actual() {
			return ErrResultNotActual
		}
		err := eq.Scan(dest)
		if err != nil {
			return err
		}
		return nil
	}
}

func checkArgs(len int) bool {
	if len>>16 > 0 {
		return false
	}

	return true
}

func (p *Pap) prepare(query *conn.Query) error {
	eq := <-p.emptyQueryChan
	eq.Mutex.Lock()
	eq.D = query.D
	err := eq.Start(
		query.SQL,
	)

	if err != nil {
		return err
	}
	cr := <-p.connReadyChan
	p.conns.list[cr].commandChan <- conn.Command{
		CommandType: conn.CommandPrepare,
		Query:       eq,
	}

	p.conns.mutex.RLock()
	for i := range p.conns.list {
		if p.conns.list[i].status == connStatusOnline && i != cr {
			eqq := <-p.emptyQueryChan
			eqq.Mutex.Lock()
			eqq.D = query.D
			err = eqq.Start(
				query.SQL,
			)
			if err != nil {
				return err
			}

			p.conns.list[i].commandChan <- conn.Command{
				CommandType: conn.CommandPrepareAsync,
				Query:       eqq,
			}
		}
	}
	p.conns.mutex.RUnlock()
	eq.Mutex.Lock()
	defer eq.Close()
	if !eq.Actual() {
		// TODO THINK
		return ErrResultNotActual
	}
	eq.AppendResultFormat()
	p.ps.list[query.SQL] = eq.D
	return eq.R.Error()
}
