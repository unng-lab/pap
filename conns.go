package pap

import (
	"sync"

	"pap/internal/conn"
)

const (
	connStatusOffline = iota
	connStatusOnline
)

type connections struct {
	mutex sync.RWMutex
	list  []connection
}

type connection struct {
	commandChan chan conn.Command
	status      int
}
