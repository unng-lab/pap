/*
 * Copyright (c) 2021-2022 UNNG Lab.
 */

package conn

import (
	"pap/internal/pgproto"
	"pap/internal/pgtype"
)

type Description struct {
	Name              string
	paramOIDs         []uint32
	resultFormats     []int16
	scanPlans         []pgtype.ScanPlan
	FieldDescriptions []pgproto.FieldDescription
}
