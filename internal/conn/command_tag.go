package conn

import "unsafe"

// CommandTag is the result of an Exec function
type CommandTag []byte

// RowsAffected returns the number of rows affected. If the CommandTag was not
// for a row affecting command (e.g. "CREATE TABLE") then it returns 0.
func (ct CommandTag) RowsAffected() int64 {
	// Find last non-digit
	idx := -1
	for i := len(ct) - 1; i >= 0; i-- {
		if ct[i] >= '0' && ct[i] <= '9' {
			idx = i
		} else {
			break
		}
	}

	if idx == -1 {
		return 0
	}

	var n int64
	for _, b := range ct[idx:] {
		n = n*10 + int64(b-'0')
	}

	return n
}

func (ct CommandTag) String() string {
	return *(*string)(unsafe.Pointer(&ct))
}

// Insert is true if the command tag starts with "INSERT".
func (ct CommandTag) Insert() bool {
	return len(ct) >= 6 &&
		ct[0] == 'I' &&
		ct[1] == 'N' &&
		ct[2] == 'S' &&
		ct[3] == 'E' &&
		ct[4] == 'R' &&
		ct[5] == 'T'
}

// Update is true if the command tag starts with "UPDATE".
func (ct CommandTag) Update() bool {
	return len(ct) >= 6 &&
		ct[0] == 'U' &&
		ct[1] == 'P' &&
		ct[2] == 'D' &&
		ct[3] == 'A' &&
		ct[4] == 'T' &&
		ct[5] == 'E'
}

// Delete is true if the command tag starts with "DELETE".
func (ct CommandTag) Delete() bool {
	return len(ct) >= 6 &&
		ct[0] == 'D' &&
		ct[1] == 'E' &&
		ct[2] == 'L' &&
		ct[3] == 'E' &&
		ct[4] == 'T' &&
		ct[5] == 'E'
}

// Select is true if the command tag starts with "SELECT".
func (ct CommandTag) Select() bool {
	return len(ct) >= 6 &&
		ct[0] == 'S' &&
		ct[1] == 'E' &&
		ct[2] == 'L' &&
		ct[3] == 'E' &&
		ct[4] == 'C' &&
		ct[5] == 'T'
}
