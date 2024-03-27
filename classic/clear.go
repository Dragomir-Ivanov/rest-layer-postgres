package pgsql

import (
	"context"
	"database/sql"
	"log/slog"

	. "github.com/doug-martin/goqu/v9"
	"github.com/rs/rest-layer/schema"
	"github.com/rs/rest-layer/schema/query"
)

func (s store) Clear(ctx context.Context, q *query.Query) (count int, err error) {
	var tx *sql.Tx

	tx, err = s.db.Begin()
	if err != nil {
		return 0, err
	}

	builder := s.dialect.Delete(s.table)

	buildDeleteWheres(s.schema, q, builder)

	sqlStr, args, err := builder.Prepared(true).ToSQL()
	if err != nil {
		return
	}

	slog.DebugContext(ctx, "pgsql.Clear", "sql", sqlStr, "args", args)

	res, err := tx.ExecContext(ctx, sqlStr, args...)
	if err != nil {
		return 0, err
	}

	cnt, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	err = tx.Commit()
	if err != nil {
		return
	}

	return int(cnt), err
}
func buildDeleteWheres(s *schema.Schema, q *query.Query, builder *DeleteDataset) {
	expressions := predicteToExpressions(s, q.Predicate)
	*builder = *builder.Where(expressions...)
}
