/*
 * Copyright (c) 2021-2022 UNNG Lab.
 */

package pgtype_test

import (
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestBPCharArrayTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "char(8)[]", []interface{}{
		&pgtype.BPCharArray{
			Elements:   nil,
			Dimensions: nil,
			Status:     pgtype.Present,
		},
		&pgtype.BPCharArray{
			Elements: []pgtype.BPChar{
				{String: "foo     ", Status: pgtype.Present},
				{Status: pgtype.Null},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Status:     pgtype.Present,
		},
		&pgtype.BPCharArray{Status: pgtype.Null},
		&pgtype.BPCharArray{
			Elements: []pgtype.BPChar{
				{String: "bar     ", Status: pgtype.Present},
				{String: "NuLL    ", Status: pgtype.Present},
				{String: `wow"quz\`, Status: pgtype.Present},
				{String: "1       ", Status: pgtype.Present},
				{String: "1       ", Status: pgtype.Present},
				{String: "null    ", Status: pgtype.Present},
			},
			Dimensions: []pgtype.ArrayDimension{
				{Length: 3, LowerBound: 1},
				{Length: 2, LowerBound: 1},
			},
			Status: pgtype.Present,
		},
		&pgtype.BPCharArray{
			Elements: []pgtype.BPChar{
				{String: " bar    ", Status: pgtype.Present},
				{String: "    baz ", Status: pgtype.Present},
				{String: "    quz ", Status: pgtype.Present},
				{String: "foo     ", Status: pgtype.Present},
			},
			Dimensions: []pgtype.ArrayDimension{
				{Length: 2, LowerBound: 4},
				{Length: 2, LowerBound: 2},
			},
			Status: pgtype.Present,
		},
	})
}
