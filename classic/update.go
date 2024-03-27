package pgsql

import (
	"context"
	"log/slog"
	"reflect"

	"github.com/doug-martin/goqu/v9"
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/rest"

	"github.com/Dragomir-Ivanov/rest-layer-postgres/internal"
)

func (s store) Update(ctx context.Context, item *resource.Item, original *resource.Item) error {
	sqlStr, args, err := s.buildUpdateQuery(item, original)
	if err != nil {
		return err
	}

	slog.DebugContext(ctx, "pgsql.Update", "sql", sqlStr, "args", args)

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

func (s store) buildUpdateQuery(i *resource.Item, o *resource.Item) (string, []any, error) {
	row := internal.CopyRow(i.Payload)
	delete(row, "id")

	for k, v := range o.Payload {
		if col, ok := row[k]; ok {
			if reflect.DeepEqual(v, col) {
				delete(row, k)
			}
		}
	}

	err := toJsonString(s.jsonFields, row)
	if err != nil {
		return "", nil, err
	}

	row["_etag"] = i.ETag
	builder := s.dialect.Update(s.table).Where(goqu.L("_etag").Eq(o.ETag), goqu.L("id").Eq(i.ID)).Set(row)

	sqlStr, args, err := builder.Prepared(true).ToSQL()
	if err != nil {
		return "", nil, err
	}

	return sqlStr, args, nil
}
