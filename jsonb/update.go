package jsonb

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"

	"github.com/doug-martin/goqu/v9"
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/rest"

	pgsql "github.com/Dragomir-Ivanov/rest-layer-postgres"
	"github.com/Dragomir-Ivanov/rest-layer-postgres/internal"
)

func (s store) Update(ctx context.Context, item *resource.Item, original *resource.Item) error {
	sqlStr, args, err := s.buildUpdateQuery(item, original)
	if err != nil {
		return err
	}

	slog.DebugContext(ctx, "pgsql.Update", "sql", sqlStr, "args", args)

	var affect sql.Result
	txCtx := pgsql.TransactionFromContext(ctx)
	if txCtx != nil {
		affect, err = txCtx.ExecContext(ctx, sqlStr, args...)
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

func (s store) buildUpdateQuery(i *resource.Item, o *resource.Item) (string, []any, error) {
	row := internal.CopyRow(i.Payload)
	delete(row, "id")

	buf := bytes.Buffer{}
	if err := json.NewEncoder(&buf).Encode(row); err != nil {
		return "", nil, err
	}

	tableRow := map[string]any{}
	tableRow["etag"] = i.ETag
	tableRow["id"] = i.ID
	tableRow["updated"] = i.Updated
	tableRow["payload"] = buf.String()

	builder := s.dialect.Update(s.table).Where(goqu.L("etag").Eq(o.ETag), goqu.L("id").Eq(i.ID)).Set(tableRow)

	sqlStr, args, err := builder.Prepared(true).ToSQL()
	if err != nil {
		return "", nil, err
	}

	return sqlStr, args, nil
}
