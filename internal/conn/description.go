package conn

import "github.com/jackc/pgproto3/v2"

type Description struct {
	Name              string
	paramOIDs         []uint32
	resultFormats     []int16
	FieldDescriptions []pgproto3.FieldDescription
}
