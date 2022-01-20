/*
 * Copyright (c) 2021-2022 UNNG Lab.
 */

package pap

import (
	"fmt"
	"testing"
)

func Test_checkArgs(t *testing.T) {
	a := 10
	b := a >> 16
	if b > 0 {
		fmt.Println(b)
	}

}
