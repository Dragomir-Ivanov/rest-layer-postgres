package jsonb

import (
	"context"
	"database/sql"
	"log/slog"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/rest"

	pgsql "github.com/Dragomir-Ivanov/rest-layer-postgres"
)

func prepareDelete(item *resource.Item) exp.Expression {
	return goqu.And(goqu.L("id").Eq(item.ID), goqu.L("etag").Eq(item.ETag))
}

func buildDelete(item *resource.Item, builder *goqu.DeleteDataset) {
	exp := prepareDelete(item)
	*builder = *builder.Where(exp)
}

func (s store) Delete(ctx context.Context, item *resource.Item) error {
	builder := s.dialect.Delete(s.table)

	buildDelete(item, builder)

	sqlStr, args, err := builder.Prepared(true).ToSQL()
	if err != nil {
		return err
	}

	slog.DebugContext(ctx, "psql.Delete", sqlStr, args)

	tx := pgsql.TransactionFromContext(ctx)
	var affect sql.Result
	if tx != nil {
		affect, err = tx.ExecContext(ctx, sqlStr, args...)
	} else {
		affect, err = s.db.ExecContext(ctx, sqlStr, args...)
	}
	if err != nil {
		return err
	}

	count, err := affect.RowsAffected()
	if err != nil {
		return err
	}

	if count != 1 {
		return rest.ErrPreconditionFailed
	}

	return nil
}
