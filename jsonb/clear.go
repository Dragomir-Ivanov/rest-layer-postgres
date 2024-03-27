package jsonb

import (
	"context"
	"database/sql"
	"log/slog"

	"github.com/doug-martin/goqu/v9"
	"github.com/rs/rest-layer/schema"
	"github.com/rs/rest-layer/schema/query"

	pgsql "github.com/Dragomir-Ivanov/rest-layer-postgres"
)

func (s store) Clear(ctx context.Context, q *query.Query) (count int, err error) {
	builder := s.dialect.Delete(s.table)

	err = buildDeleteWheres(s.schema, q, builder)
	if err != nil {
		return
	}

	sqlStr, args, err := builder.Prepared(true).ToSQL()
	if err != nil {
		return
	}

	slog.DebugContext(ctx, "pgsql.Clear", "sql", sqlStr, "args", args)

	tx := pgsql.TransactionFromContext(ctx)
	var res sql.Result
	if tx != nil {
		res, err = tx.ExecContext(ctx, sqlStr, args...)
	} else {
		res, err = s.db.ExecContext(ctx, sqlStr, args...)
	}
	if err != nil {
		return 0, err
	}

	cnt, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(cnt), err
}
func buildDeleteWheres(s *schema.Schema, q *query.Query, builder *goqu.DeleteDataset) error {
	expressions, err := predicteToExpressions("", s, q.Predicate)
	if err != nil {
		return err
	}
	*builder = *builder.Where(expressions...)
	return nil
}
