package internal

import (
	"fmt"

	"github.com/rs/rest-layer/schema"
)

func FixSchemaTypes(s *schema.Schema, payload map[string]any) error {
	for key, value := range payload {
		if fv, ok := s.Fields[key]; ok {
			val, err := evalField(fv, value)
			if err != nil {
				return fmt.Errorf("field %s: %w", key, err)
			}
			if val != nil {
				payload[key] = val
			}
		}
	}
	return nil
}

func evalField(f schema.Field, payload any) (any, error) {
	switch validator := f.Validator.(type) {
	case *schema.Object:
		switch v := payload.(type) {
		case map[string]any:
			if err := FixSchemaTypes(validator.Schema, v); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("expected object, got %T", v)
		}

	case *schema.Dict:
		switch v := payload.(type) {
		case map[string]any:
			for key, item := range v {
				if val, err := evalField(validator.Values, item); err != nil {
					return nil, err
				} else if val != nil {
					v[key] = val
				}
			}
		default:
			return nil, fmt.Errorf("expected dict, got %T", v)
		}

	case *schema.Array:
		switch v := payload.(type) {
		case []any:
			for i, item := range v {
				if val, err := evalField(validator.Values, item); err != nil {
					return nil, err
				} else if val != nil {
					v[i] = val
				}
			}
		default:
			return nil, fmt.Errorf("expected array, got %T", v)
		}

	default:
		if serializer, ok := validator.(schema.FieldSerializer); ok {
			if fs, err := serializer.Serialize(payload); err != nil {
				return nil, err
			} else {
				return fs, nil
			}
		}
	}
	return nil, nil
}
