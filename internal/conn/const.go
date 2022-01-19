package conn

const (
	statusUnknown byte = iota
	statusUninitialized
	statusConnecting
	statusClosed
	statusIdle
	statusBusy
)

const (
	CommandUnknown byte = iota
	CommandQuery
	CommandPreparedQuery
	CommandPrepare
	CommandPrepareAsync
	CommandFuncCache
	CommandConnect
	CommandDisconnect
)

const wbufLen = 1024

// PostgreSQL format codes
const (
	TextFormatCode   = 0
	BinaryFormatCode = 1
)
