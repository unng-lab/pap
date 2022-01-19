package conn

import (
	"github.com/jackc/pgtype"
)

type ResultFunc func(dest ...interface{}) error

// Result is the saved query response that is returned by calling Read on a ResultReader.
type Result struct {
	rowValues [][][]byte
	err       error
	connInfo  *pgtype.ConnInfo
	//scanPlans         []pgtype.ScanPlan
	commandTag       CommandTag
	commandConcluded bool
}

func (r *Result) concludeCommand(commandTag CommandTag, err error) {
	// Keep the first error that is recorded. Store the error before checking if the command is already concluded to
	// allow for receiving an error after CommandComplete but before ReadyForQuery.
	if err != nil && r.err == nil {
		r.err = err
	}

	if r.commandConcluded {
		return
	}

	r.commandTag = commandTag
}

func (r *Result) Error() error {
	return r.err
}
