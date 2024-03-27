package jsonb

import (
	"reflect"
	"testing"

	"github.com/rs/rest-layer/schema"
)

func TestStore_buildCreateQuery(t *testing.T) {
	schema := &schema.Schema{
		Fields: schema.Fields{
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

	query, params, err := buildCreateQuery("table", schema)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	expectedQuery := `CREATE TABLE IF NOT EXISTS "table" (id VARCHAR(24),etag VARCHAR(32),updated TIMESTAMP,payload JSONB,PRIMARY KEY(id))`
	if query != expectedQuery {
		t.Errorf("Expected query: %s, got: %s", expectedQuery, query)
	}

	var expectedParams []any
	if !reflect.DeepEqual(params, expectedParams) {
		t.Errorf("Expected params: %#v, got: %#v", expectedParams, params)
	}
}
