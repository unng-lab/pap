package conn

import (
	"os"
	"strconv"
)

const (
	maxResultSaveDurationInSecondsConst int64 = 500000000000
)

var MaxResultSaveDurationInNanoseconds int64

func init() {
	maxDur := os.Getenv("PAP_MAX_RESULT_SAVE_DURATION_IN_SECONDS")
	maxDurInt, err := strconv.Atoi(maxDur)
	if maxDur != "" && err == nil {
		MaxResultSaveDurationInNanoseconds = int64(maxDurInt * 1000000000)
	} else {
		MaxResultSaveDurationInNanoseconds = maxResultSaveDurationInSecondsConst
	}

}
