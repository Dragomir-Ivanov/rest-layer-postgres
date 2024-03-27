package jsonb

import (
	"context"
	"database/sql"

	"github.com/doug-martin/goqu/v9"
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema"

	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
)

type PostgresStorer interface {
	resource.Storer
	AutoMigrate() error
}

type store struct {
	table   string
	db      *sql.DB
	dialect goqu.DialectWrapper
	schema  *schema.Schema
}

func NewStore(table string, db *sql.DB, sc *schema.Schema) PostgresStorer {
	s := &store{
		table:   table,
		db:      db,
		dialect: goqu.Dialect("postgres"),
		schema:  sc,
	}

	return s
}

func (s *store) AutoMigrate() error {
	return s.Migrate(context.TODO(), s.schema)
}
