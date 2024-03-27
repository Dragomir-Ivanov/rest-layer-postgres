package pgsql

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"strings"

	pgsql "github.com/Dragomir-Ivanov/rest-layer-postgres"
	"github.com/rotisserie/eris"
	"github.com/rs/rest-layer/schema"
)

func (s store) Migrate(ctx context.Context, sc *schema.Schema) (err error) {
	sqlQuery, sqlParams, err := buildCreateQuery(s.table, sc)
	if err != nil {
		return err
	}

	slog.DebugContext(ctx, "psql.Migrate", "sql", sqlQuery, "args", sqlParams)

	_, err = s.db.ExecContext(ctx, sqlQuery, sqlParams...)
	return err
}

func buildCreateQuery(tableName string, s *schema.Schema) (sqlQuery string, sqlParams []any, err error) {
	schemaQuery, schemaParams, err := buildCreateTable(s)
	if err != nil {
		return "", []any{}, err
	}

	sqlQuery = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (%s,PRIMARY KEY(id))`, tableName, schemaQuery)
	sqlParams = append(sqlParams, schemaParams...)

	return sqlQuery, sqlParams, nil
}

func buildCreateTable(s *schema.Schema) (sqlQuery string, sqlParams []any, err error) {
	fieldStrings := make([]string, 0, len(s.Fields))

	for fieldName, field := range s.Fields {
		if fieldName == "id" && reflect.DeepEqual(field, pgsql.SerialID) {
			fieldStrings = append(fieldStrings, "id SERIAL")
			continue
		}

		fieldName = `"` + fieldName + `"`
		pgType, err := schemaFieldValidatorToPGType(&field)
		if err != nil {
			return "", []any{}, eris.Wrapf(err, "failed to convert field %s to pg type", fieldName)
		}

		fieldStrings = append(fieldStrings, fieldName+" "+pgType)
	}

	fieldStrings = append(fieldStrings, "_updated TIMESTAMP NOT NULL")
	fieldStrings = append(fieldStrings, "_etag CHAR(32) NOT NULL")

	return strings.Join(fieldStrings, ","), []any{}, nil
}

func schemaFieldValidatorToPGType(field *schema.Field) (string, error) {
	pgType := ""
	switch f := field.Validator.(type) {
	case *schema.String:
		if f.MaxLen > 0 {
			pgType = fmt.Sprintf("VARCHAR(%d)", f.MaxLen)
		} else {
			pgType = "VARCHAR"
		}
	case *schema.Integer:
		pgType = getIntegerScale(f)
	case *schema.Float:
		pgType = "DOUBLE PRECISION"
	case *schema.Bool:
		pgType = "BOOLEAN"
	case *schema.Time:
		pgType = "TIMESTAMP"
	case *schema.URL, *schema.IP, *schema.Password:
		pgType = "VARCHAR"
	case *schema.Reference:
		field := f.GetField("id")
		var err error
		pgType, err = schemaFieldValidatorToPGType(field)
		if err != nil {
			// TODO: this is a hack to get around the fact that we don't have a way to get the type of a reference field
			pgType = "VARCHAR"
		}
	case *schema.Object, *schema.Dict, *schema.Array:
		pgType = "JSONB"
	case pgsql.PostgresTyper:
		pgType = f.PostgresType()
	case nil:
		return "", fmt.Errorf("validator required")
	default:
		return "", fmt.Errorf("unsupported field validator type: %+v", f)
	}

	if field.Required {
		pgType += " NOT NULL"
	}

	return pgType, nil
}

func getIntegerScale(f *schema.Integer) string {
	if f.Boundaries == nil {
		return "INTEGER"
	}
	if f.Boundaries.Max == 0 || f.Boundaries.Max > 1<<31-1 {
		return "BIGINT"
	}
	if f.Boundaries.Max > 1<<15-1 {
		return "INTEGER"
	}
	return "SMALLINT"
}
