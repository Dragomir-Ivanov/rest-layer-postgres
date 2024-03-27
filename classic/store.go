package pgsql

import (
	"context"
	"database/sql"

	"github.com/doug-martin/goqu/v9"
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema"
)

type PostgresStorer interface {
	resource.Storer
	AutoMigrate() error
}

type store struct {
	table      string
	db         *sql.DB
	dialect    goqu.DialectWrapper
	schema     *schema.Schema
	jsonFields schema.Fields
}

func NewStore(table string, db *sql.DB, sc *schema.Schema) PostgresStorer {
	s := &store{
		table:      table,
		db:         db,
		dialect:    goqu.Dialect("postgres"),
		schema:     sc,
		jsonFields: getJsonFields(sc.Fields),
	}

	return s
}

func getJsonFields(fields schema.Fields) schema.Fields {
	jsonColumns := make(map[string]schema.Field, 0)
	for name, field := range fields {
		switch field.Validator.(type) {
		case *schema.Object, *schema.Array, *schema.Dict:
			jsonColumns[name] = field
		case nil:
			if field.Schema != nil {
				jsonColumns[name] = field
			}
		}
	}
	return jsonColumns
}

func (s *store) AutoMigrate() error {
	return s.Migrate(context.TODO(), s.schema)
}
