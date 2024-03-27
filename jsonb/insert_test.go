package jsonb

import (
	"reflect"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema"

	pgsql "github.com/Dragomir-Ivanov/rest-layer-postgres"
)

func TestStore_prepareInsertQuery(t *testing.T) {
	t.Run("insert: nested", func(t *testing.T) {
		schema := &schema.Schema{
			Fields: schema.Fields{
				"id": pgsql.IDField,
				"name": {
					Validator: &schema.String{},
				},
				"age": {
					Validator: &schema.Integer{},
				},
				"address": {
					Validator: &schema.Dict{},
				},
			},
		}
		item := &resource.Item{
			ID: "1234567890abcdefjhij",
			Payload: map[string]interface{}{
				"name": "John",
				"age":  30,
				"address": map[string]interface{}{
					"city":  "New York",
					"state": "NY",
				},
			},
			ETag: "123",
		}

		sqlStr, args, err := prepareInsertQuery(goqu.Dialect("postgres"), schema, "table", item)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		expectedQuery := `INSERT INTO "table" ("etag", "id", "payload", "updated") VALUES ($1, $2, $3, $4)`
		expectedArgs := []any{
			"123", "1234567890abcdefjhij",
			`{"address":{"city":"New York","state":"NY"},"age":30,"name":"John"}
`, time.Time{},
		}
		if sqlStr != expectedQuery {
			t.Errorf("Expected query: %s, got: %s", expectedQuery, sqlStr)
		}
		if !reflect.DeepEqual(args, expectedArgs) {
			t.Errorf("Expected args: %v, got: %v", expectedArgs, args)
		}
	})
}
