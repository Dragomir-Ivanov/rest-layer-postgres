package pgsql

import (
	"context"
	"database/sql"
	"log/slog"
	"reflect"

	"github.com/doug-martin/goqu/v9"
	"github.com/rs/rest-layer/resource"

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

func (s store) insertOne(ctx context.Context, item *resource.Item) error {
	row := internal.CopyRow(item.Payload)
	row["_etag"] = item.ETag
	row["_updated"] = item.Updated

	useSerial := reflect.DeepEqual(s.schema.Fields["id"], pgsql.SerialID)

	if useSerial {
		delete(row, "id")
	}

	// Converting json node to string for adapting goqu framework
	if err := toJsonString(s.jsonFields, row); err != nil {
		return err
	}

	builder := s.dialect.Insert(s.table)

	if useSerial {
		builder = builder.Returning(goqu.L("id"))
	}

	sqlStr, args, err := builder.Prepared(true).Rows(row).ToSQL()
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

	if useSerial {
		var id string
		if err = result.Scan(&id); err != nil {
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
