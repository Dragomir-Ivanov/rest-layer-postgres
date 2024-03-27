package pgsql

import (
	"context"
	"log/slog"

	. "github.com/doug-martin/goqu/v9"
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/rest"
)

func (s store) Delete(ctx context.Context, item *resource.Item) error {
	sqlStr, args, err := s.dialect.Delete(s.table).Where(L("id").Eq(item.ID), L("_etag").Eq(item.ETag)).Prepared(true).ToSQL()
	if err != nil {
		return err
	}

	slog.DebugContext(ctx, "pgsql.Delete", "sql", sqlStr, "args", args)

	affect, err := s.db.ExecContext(ctx, sqlStr, args...)
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
