/*
 * Copyright (c) 2021-2022 UNNG Lab.
 */

package pgproto

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/jackc/pgio"
)

type Describe struct {
	ObjectType byte // 'S' = prepared statement, 'P' = portal
	Name       string
}

// Frontend identifies this message as sendable by a PostgreSQL frontend.
func (*Describe) Frontend() {}

// Decode decodes src into dst. src must contain the complete message with the exception of the initial 1 byte message
// type identifier and 4 byte message length.
func (dst *Describe) Decode(src []byte) error {
	if len(src) < 2 {
		return &invalidMessageFormatErr{messageType: "Describe"}
	}

	dst.ObjectType = src[0]
	rp := 1

	idx := bytes.IndexByte(src[rp:], 0)
	if idx != len(src[rp:])-1 {
		return &invalidMessageFormatErr{messageType: "Describe"}
	}

	dst.Name = string(src[rp : len(src)-1])

	return nil
}

// Encode encodes src into dst. dst will include the 1 byte message type identifier and the 4 byte message length.
func (src *Describe) Encode(dst []byte) []byte {
	dst = append(dst, 'D')
	sp := len(dst)
	dst = pgio.AppendInt32(dst, -1)

	dst = append(dst, src.ObjectType)
	dst = append(dst, src.Name...)
	dst = append(dst, 0)

	pgio.SetInt32(dst[sp:], int32(len(dst[sp:])))

	return dst
}

// MarshalJSON implements encoding/json.Marshaler.
func (src Describe) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type       string
		ObjectType string
		Name       string
	}{
		Type:       "Describe",
		ObjectType: string(src.ObjectType),
		Name:       src.Name,
	})
}

// UnmarshalJSON implements encoding/json.Unmarshaler.
func (dst *Describe) UnmarshalJSON(data []byte) error {
	// Ignore null, like in the main JSON package.
	if string(data) == "null" {
		return nil
	}

	var msg struct {
		ObjectType string
		Name       string
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		return err
	}
	if len(msg.ObjectType) != 1 {
		return errors.New("invalid length for Describe.ObjectType")
	}

	dst.ObjectType = byte(msg.ObjectType[0])
	dst.Name = msg.Name
	return nil
}
