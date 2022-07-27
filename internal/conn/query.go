/*
 * Copyright (c) 2021-2022 UNNG Lab.
 */

package conn

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"sync"

	"pap/internal/pgproto"
	"pap/internal/pgtype"
)

type Query struct {
	SQL             string
	Args            []interface{}
	paramFormats    []int16
	paramValues     [][]byte
	paramValueBytes []byte

	startTime      int64
	D              *Description
	R              Result
	Mutex          sync.RWMutex
	emptyQueryChan chan *Query
	used           bool
}

func NewQuery(connInfo *pgtype.ConnInfo, emptyQueryChan chan *Query) *Query {
	return &Query{
		SQL:             "",
		Args:            make([]interface{}, 0, 16),
		paramFormats:    make([]int16, 0, 128),
		paramValues:     make([][]byte, 0, 128),
		paramValueBytes: make([]byte, 0, 512),
		R: Result{
			connInfo:  connInfo,
			rowValues: make([][]byte, 0, 256),
		},
		D: &Description{
			FieldDescriptions: make([]pgproto.FieldDescription, 0, 128),
			paramOIDs:         make([]uint32, 0, 128),
			resultFormats:     make([]int16, 0, 128),
		},
		emptyQueryChan: emptyQueryChan,
		used:           false,
	}
}

func (q *Query) Actual() bool {
	return (nanotime()-q.startTime) < MaxResultSaveDurationInNanoseconds || q.used
}

func (q *Query) Close() {
	q.used = true
	q.Mutex.Unlock()
	q.Return()
}

func (q *Query) ready() {
	q.Mutex.Unlock()
}

func (q *Query) Return() {
	q.emptyQueryChan <- q
}

func (q *Query) Start(sql string, args ...interface{}) error {
	q.paramValues = q.paramValues[:0]
	q.paramValueBytes = q.paramValueBytes[:0]
	q.paramFormats = q.paramFormats[:0]
	q.Args = q.Args[:0]
	q.R.rowValues = q.R.rowValues[:0]

	q.D.FieldDescriptions = q.D.FieldDescriptions[:0]
	q.D.paramOIDs = q.D.paramOIDs[:0]
	q.D.resultFormats = q.D.resultFormats[:0]

	q.R.commandConcluded = false

	if q.R.err != nil {
		q.R.err = nil
	}

	q.startTime = nanotime()
	q.SQL = sql
	q.Args = append(q.Args, args...)
	err := q.convertDriverValuers()
	if err != nil {
		return err
	}

	return nil
}

// NoticeHandler is a function that can handle notices received from the PostgreSQL server. Notices can be received at
// any time, usually during handling of a query response. The *connection is provided so the handler is aware of the origin
// of the notice, but it must not invoke any query method. Be aware that this is distinct from LISTEN/NOTIFY
// notification.
// TODO notice
//type NoticeHandler func(*connection, *Notice)

// NotificationHandler is a function that can handle notifications received from the PostgreSQL server. Notifications
// can be received at any time, usually during handling of a query response. The *connection is provided so the handler is
// aware of the origin of the notice, but it must not invoke any query method. Be aware that this is distinct from a
// notice event.
// TODO notice
//type NotificationHandler func(*connection, *Notification)

func (q *Query) Scan(dest interface{}) error {

	if q.R.err != nil {
		err := fmt.Errorf("error: %s", q.R.err.Error())
		return err
	}

	//if len(q.D.FieldDescriptions) != len(q.R.rowValues) {
	//	err := fmt.Errorf("number of field descriptions must equal number of values, got %d and %d", len(q.D.FieldDescriptions), len(q.R.rowValues))
	//	return err
	//}

	//if len(q.D.FieldDescriptions) != len(dest) {
	//	err := fmt.Errorf("number of field descriptions must equal number of destinations, got %d and %d", len(q.D.FieldDescriptions), len(dest))
	//	return err
	//}

	columnsCount := len(q.D.FieldDescriptions)
	rowsCount := len(q.R.rowValues) / columnsCount

	s := reflect.Indirect(reflect.ValueOf(dest))

	s.Set(reflect.AppendSlice(s, reflect.MakeSlice(reflect.TypeOf(dest).Elem(), rowsCount, rowsCount)))

	for r := 0; r < rowsCount; r++ {
		if q.D.scanPlans == nil {
			q.D.scanPlans = make([]pgtype.ScanPlan, 0, columnsCount)
			for i := 0; i < columnsCount; i++ {
				q.D.scanPlans = append(q.D.scanPlans, q.R.connInfo.PlanScan(
					q.D.FieldDescriptions[i].DataTypeOID,
					q.D.FieldDescriptions[i].Format,
					s.Index(r).Field(i).Addr().Interface(),
				))
			}
		}

		for i := 0; i < columnsCount; i++ {
			err := q.D.scanPlans[i].Scan(
				q.R.connInfo,
				q.D.FieldDescriptions[i].DataTypeOID,
				q.D.FieldDescriptions[i].Format,
				q.R.rowValues[r*columnsCount+i],
				s.Index(r).Field(i).Addr().Interface(),
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (q *Query) convertDriverValuers() error {
	for i := range q.Args {
		switch arg := q.Args[i].(type) {
		case pgtype.BinaryEncoder:
		case pgtype.TextEncoder:
		case driver.Valuer:
			v, err := callValuerValue(arg)
			if err != nil {
				return err
			}
			q.Args[i] = v
		}
	}
	return nil
}

// From database/sql/convert.go

var valuerReflectType = reflect.TypeOf((*driver.Valuer)(nil)).Elem()

// callValuerValue returns vr.Value(), with one exception:
// If vr.Value is an auto-generated method on a pointer type and the
// pointer is nil, it would panic at runtime in the panic-wrap
// method. Treat it like nil instead.
// Issue 8415.
//
// This is so people can implement driver.Value on value types and
// still use nil pointers to those types to mean nil/NULL, just like
// string/*string.
//
// This function is mirrored in the database/sql/driver package.
func callValuerValue(vr driver.Valuer) (v driver.Value, err error) {
	if rv := reflect.ValueOf(vr); rv.Kind() == reflect.Ptr &&
		rv.IsNil() &&
		rv.Type().Elem().Implements(valuerReflectType) {
		return nil, nil
	}
	return vr.Value()
}

func (q *Query) AppendParam(i int) error {
	q.paramFormats = append(q.paramFormats, q.chooseParameterFormatCode(i))

	v, err := q.encodeExtendedParamValue(i)
	if err != nil {
		return err
	}
	q.paramValues = append(q.paramValues, v)

	return nil
}

// chooseParameterFormatCode determines the correct format code for an
// argument to a prepared statement. It defaults to TextFormatCode if no
// determination can be made.
func (q *Query) chooseParameterFormatCode(i int) int16 {
	switch arg := q.Args[i].(type) {
	case pgtype.ParamFormatPreferrer:
		return arg.PreferredParamFormat()
	case pgtype.BinaryEncoder:
		return BinaryFormatCode
	case string, *string, pgtype.TextEncoder:
		return TextFormatCode
	}

	return q.R.connInfo.ParamFormatCodeForOID(q.D.paramOIDs[i])
}

func (q *Query) encodeExtendedParamValue(i int) ([]byte, error) {
	if q.Args[i] == nil {
		return nil, nil
	}

	refVal := reflect.ValueOf(q.Args[i])
	argIsPtr := refVal.Kind() == reflect.Ptr

	if argIsPtr && refVal.IsNil() {
		return nil, nil
	}

	var err error

	if arg, ok := q.Args[i].(string); ok {
		return []byte(arg), nil
	}

	if q.paramFormats[i] == TextFormatCode {
		if arg, ok := q.Args[i].(pgtype.TextEncoder); ok {
			q.paramValueBytes, err = arg.EncodeText(q.R.connInfo, q.paramValueBytes)
			if err != nil {
				return nil, err
			}
			if q.paramValueBytes == nil {
				return nil, nil
			}
			return q.paramValueBytes, nil
		}
	} else if q.paramFormats[i] == BinaryFormatCode {
		if arg, ok := q.Args[i].(pgtype.BinaryEncoder); ok {
			if len(q.paramValueBytes) > 0 {
				panic("pzdc")
			}
			q.paramValueBytes, err = arg.EncodeBinary(q.R.connInfo, q.paramValueBytes)
			if err != nil {
				return nil, err
			}
			if q.paramValueBytes == nil {
				return nil, nil
			}
			return q.paramValueBytes, nil
		}
	}

	if argIsPtr {
		// We have already checked that arg is not pointing to nil,
		// so it is safe to dereference here.
		q.Args[i] = refVal.Elem().Interface()
		return q.encodeExtendedParamValue(i)
	}

	if dt, ok := q.R.connInfo.DataTypeForOID(q.D.paramOIDs[i]); ok {
		value := dt.Value
		err := value.Set(q.Args[i])
		q.Args[i] = value
		if err != nil {
			{
				if arg, ok := q.Args[i].(driver.Valuer); ok {
					v, err := callValuerValue(arg)
					q.Args[i] = v
					if err != nil {
						return nil, err
					}
					return q.encodeExtendedParamValue(i)
				}
			}

			return nil, err
		}

		return q.encodeExtendedParamValue(i)
	}
	// There is no data type registered for the destination OID, but maybe there is data type registered for the arg
	// type. If so use its text encoder (if available).
	if dt, ok := q.R.connInfo.DataTypeForValue(q.Args[i]); ok {
		value := dt.Value
		if textEncoder, ok := value.(pgtype.TextEncoder); ok {
			err := value.Set(q.Args[i])
			if err != nil {
				return nil, err
			}

			q.paramValueBytes, err = textEncoder.EncodeText(q.R.connInfo, q.paramValueBytes)
			if err != nil {
				return nil, err
			}
			if q.paramValueBytes == nil {
				return nil, nil
			}
			return q.paramValueBytes, nil
		}
	}

	if strippedArg, ok := stripNamedType(&refVal); ok {
		q.Args[i] = strippedArg
		return q.encodeExtendedParamValue(i)
	}

	return nil, SerializationError(fmt.Sprintf("Cannot encode %T into oid %v - %T must implement Encoder or be converted to a string", q.Args[i], q.D.paramOIDs[i], q.Args[i]))
}

func stripNamedType(val *reflect.Value) (interface{}, bool) {
	switch val.Kind() {
	case reflect.Int:
		convVal := int(val.Int())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Int8:
		convVal := int8(val.Int())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Int16:
		convVal := int16(val.Int())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Int32:
		convVal := int32(val.Int())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Int64:
		convVal := val.Int()
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Uint:
		convVal := uint(val.Uint())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Uint8:
		convVal := uint8(val.Uint())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Uint16:
		convVal := uint16(val.Uint())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Uint32:
		convVal := uint32(val.Uint())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Uint64:
		convVal := val.Uint()
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.String:
		convVal := val.String()
		return convVal, reflect.TypeOf(convVal) != val.Type()
	}

	return nil, false
}

func (q *Query) AppendResultFormat() {
	for i := range q.D.FieldDescriptions {
		q.D.resultFormats = append(q.D.resultFormats, q.R.connInfo.ResultFormatCodeForOID(q.D.FieldDescriptions[i].DataTypeOID))
	}

}
