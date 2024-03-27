package jsonb

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"reflect"

	"github.com/doug-martin/goqu/v9"
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema"

	pgsql "github.com/Dragomir-Ivanov/rest-layer-postgres"
	"github.com/Dragomir-Ivanov/rest-layer-postgres/internal"
)

func (s store) Insert(ctx context.Context, items []*resource.Item) (err error) {
	for _, item := range items {
		if err = s.insertOne(ctx, item); err != nil {
			return
		}
	}

	return
}

func prepareInsertQuery(dialect goqu.DialectWrapper, s *schema.Schema, table string, item *resource.Item) (string, []any, error) {
	row := internal.CopyRow(item.Payload)
	delete(row, "id")

	buf := bytes.Buffer{}
	if err := json.NewEncoder(&buf).Encode(row); err != nil {
		return "", nil, err
	}

	tableRow := map[string]any{}
	tableRow["etag"] = item.ETag
	tableRow["id"] = item.ID
	tableRow["updated"] = item.Updated
	tableRow["payload"] = buf.String()

	builder := dialect.Insert(table)
	useSerial := reflect.DeepEqual(s.Fields["id"], pgsql.SerialID)
	if useSerial {
		builder = builder.Returning(goqu.L("id"))
	}
	return builder.Prepared(true).Rows(tableRow).ToSQL()
}

func (s store) insertOne(ctx context.Context, item *resource.Item) error {
	sqlStr, args, err := prepareInsertQuery(s.dialect, s.schema, s.table, item)
	if err != nil {
		return err
	}

	slog.DebugContext(ctx, "pgsql.Insert", "sql", sqlStr, "args", args)

	tx := pgsql.TransactionFromContext(ctx)
	var result *sql.Row
	if tx != nil {
		result = tx.QueryRowContext(ctx, sqlStr, args...)
	} else {
		result = s.db.QueryRowContext(ctx, sqlStr, args...)
	}

	if result.Err() != nil {
		return result.Err()
	}

	useSerial := reflect.DeepEqual(s.schema.Fields["id"], pgsql.SerialID)
	if useSerial {
		var id string
		if err := result.Scan(&id); err != nil {
			return err
		}
		item.Payload["id"] = id
		item.ID = id
	} else {
		// Consume the result, in order to rows to be closed
		result.Scan()
	}

	return nil
}
