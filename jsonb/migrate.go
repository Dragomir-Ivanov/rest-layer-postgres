package jsonb

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	pgsql "github.com/Dragomir-Ivanov/rest-layer-postgres"
	"github.com/rs/rest-layer/schema"
)

func (s store) Migrate(ctx context.Context, sc *schema.Schema) (err error) {
	sqlQuery, sqlParams, err := buildCreateQuery(s.table, sc)
	if err != nil {
		return err
	}

	_, err = s.db.ExecContext(ctx, sqlQuery, sqlParams...)
	return err
}

func buildCreateQuery(tableName string, s *schema.Schema) (sqlQuery string, sqlParams []any, err error) {
	schemaQuery := buildCreateTable(s)
	sqlQuery = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (%s,PRIMARY KEY(id))`, tableName, schemaQuery)
	return sqlQuery, sqlParams, nil
}

func buildCreateTable(s *schema.Schema) string {
	fieldStrings := make([]string, 0, len(s.Fields))

	if field, ok := s.Fields["id"]; ok && reflect.DeepEqual(field, pgsql.SerialID) {
		fieldStrings = append(fieldStrings, "id SERIAL")
	} else {
		fieldStrings = append(fieldStrings, "id VARCHAR(24)")
	}
	fieldStrings = append(fieldStrings, "etag VARCHAR(32)")
	fieldStrings = append(fieldStrings, "updated TIMESTAMP")
	fieldStrings = append(fieldStrings, "payload JSONB")

	return strings.Join(fieldStrings, ",")
}
