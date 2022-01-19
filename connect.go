package pap

import "pap/internal/conn"

func (p *pap) connect(count int) {
	p.conns.mutex.Lock()
	defer p.conns.mutex.Unlock()
	for i := 0; i < count; i++ {
		if p.conns.list[i].status == connStatusOffline {
			p.conns.list[i].commandChan <- conn.Command{
				CommandType: conn.CommandConnect,
				Body:        p.config,
			}
			p.conns.list[i].status = connStatusOnline
		}
	}
}
