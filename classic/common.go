package pgsql

import (
	"bytes"
	"encoding/json"

	"github.com/rs/rest-layer/schema"
)

func toJsonString(jsonFields schema.Fields, row map[string]any) error {
	if len(jsonFields) == 0 {
		return nil
	}

	buf := bytes.Buffer{}
	for name := range jsonFields {
		if col, ok := row[name]; ok {
			if err := json.NewEncoder(&buf).Encode(col); err != nil {
				return err
			}
			row[name] = buf.String()
			buf.Reset()
		}
	}

	return nil
}
