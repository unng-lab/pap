/*
 * Copyright (c) 2021-2022 UNNG Lab.
 */

package conn

import (
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
)

type Description struct {
	Name              string
	paramOIDs         []uint32
	resultFormats     []int16
	scanPlans         []pgtype.ScanPlan
	FieldDescriptions []pgproto3.FieldDescription
}
