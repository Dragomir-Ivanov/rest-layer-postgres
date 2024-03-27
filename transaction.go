package pgsql

import (
	"context"
	"database/sql"
)

type TransactionContext interface {
	context.Context
}

type transactionContext struct {
	context.Context
	*sql.Tx
}

type transactionKey struct {
}

// NewSessionContext creates a new SessionContext associated with the given Context and Session parameters.
func NewTransactionContext(ctx context.Context, tx *sql.Tx) TransactionContext {
	return &transactionContext{
		Context: context.WithValue(ctx, transactionKey{}, tx),
		Tx:      tx,
	}
}

// SessionFromContext extracts the mongo.Session object stored in a Context. This can be used on a SessionContext that
// was created implicitly through one of the callback-based session APIs or explicitly by calling NewSessionContext. If
// there is no Session stored in the provided Context, nil is returned.
func TransactionFromContext(ctx context.Context) *sql.Tx {
	val := ctx.Value(transactionKey{})
	if val == nil {
		return nil
	}

	tx, ok := val.(*sql.Tx)
	if !ok {
		return nil
	}

	return tx
}
