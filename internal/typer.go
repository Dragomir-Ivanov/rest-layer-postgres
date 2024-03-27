package internal

import (
	"github.com/rs/rest-layer/schema"

	pgsql "github.com/Dragomir-Ivanov/rest-layer-postgres"
)

func PgtypeFromField(s *schema.Schema, parent, name string) string {
	if s == nil {
		return ""
	}

	if parent != "" {
		name = parent + "." + name
	}

	field := s.GetField(name)
	var pgtype string
	if field != nil {
		if postgresTyper, ok := field.Validator.(pgsql.PostgresTyper); ok {
			pgtype = postgresTyper.PostgresType()
		}
	}
	return pgtype
}
