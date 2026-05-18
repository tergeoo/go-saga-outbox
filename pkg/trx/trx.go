package trx

import (
	"context"
	"database/sql"
	"log/slog"

	"github.com/jmoiron/sqlx"
)

type ctxKeyTrx string

var ctxTxKey ctxKeyTrx = "trx"

type Trx interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

type TrxBeginner interface {
	BeginContext(ctx context.Context) (context.Context, *sqlx.Tx, error)
}

type Transaction struct {
	db *sqlx.DB
}

func NewTransaction(db *sqlx.DB) *Transaction {
	return &Transaction{db: db}
}

func (t *Transaction) BeginContext(ctx context.Context) (context.Context, *sqlx.Tx, error) {
	tx, err := t.db.BeginTxx(ctx, nil)
	if err != nil {
		return ctx, nil, err
	}
	return context.WithValue(ctx, ctxTxKey, tx), tx, nil
}

func (t *Transaction) FromContext(ctx context.Context) Trx {
	if trx, ok := ctx.Value(ctxTxKey).(Trx); ok && trx != nil {
		return trx
	}
	return t.db
}

func (t *Transaction) Run(ctx context.Context, fn func(ctx context.Context) error) (err error) {
	return runInTx(ctx, t, fn)
}

func Exec[T any](ctx context.Context, b TrxBeginner, fn func(ctx context.Context) (T, error)) (result T, err error) {
	err = runInTx(ctx, b, func(ctx context.Context) error {
		result, err = fn(ctx)
		return err
	})

	return result, err
}

func runInTx(ctx context.Context, b TrxBeginner, fn func(ctx context.Context) error) error {
	ctx, tx, err := b.BeginContext(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			if txErr := tx.Rollback(); txErr != nil {
				slog.Error(
					"transaction rollback failed after panic",
					"error", txErr,
				)
			}
			panic(p)
		}
		if err != nil {
			if txErr := tx.Rollback(); txErr != nil {
				slog.Error(
					"transaction rollback failed after panic",
					"error", txErr,
				)
			}
			return
		}
		if e := tx.Commit(); e != nil {
			err = e
		}
	}()
	err = fn(ctx)
	return err
}
