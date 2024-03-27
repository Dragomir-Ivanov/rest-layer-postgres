package internal

import (
	"reflect"
	"testing"
	"time"

	clone "github.com/huandu/go-clone/generic"
	"github.com/rs/rest-layer/schema"
)

type testTime struct{}

func (t testTime) Validate(value any) (any, error) {
	if v, ok := value.(time.Time); ok {
		return v, nil
	}

	//Check for string
	if v, ok := value.(string); ok {
		v, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return nil, err
		}
		return v, nil
	}

	return nil, nil
}

func (t testTime) Serialize(value any) (any, error) {
	return t.Validate(value)
}

func Test_evalSchema(t *testing.T) {
	var testSchema = schema.Schema{
		Fields: schema.Fields{
			"field1": {
				Validator: &schema.String{},
			},
			"field-time": {
				Validator: &testTime{},
			},
			"field-time-object": {
				Validator: &schema.Object{
					Schema: &schema.Schema{
						Fields: schema.Fields{
							"sub-time": {
								Validator: &testTime{},
							},
							"sub-object": {
								Validator: &schema.Object{
									Schema: &schema.Schema{
										Fields: schema.Fields{
											"sub-sub-time": {
												Validator: &testTime{},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			"field-dict": {},
			"field-dict-time": {
				Validator: &schema.Dict{
					Values: schema.Field{
						Validator: &testTime{},
					},
				},
			},
			"field-array-string": {
				Validator: &schema.Array{
					Values: schema.Field{
						Validator: &schema.String{},
					},
				},
			},
			"field-array-time": {
				Validator: &schema.Array{
					Values: schema.Field{
						Validator: &testTime{},
					},
				},
			},
			"field-array-object-string": {
				Validator: &schema.Array{
					Values: schema.Field{
						Validator: &schema.Object{
							Schema: &schema.Schema{
								Fields: schema.Fields{
									"sub-string": {
										Validator: &schema.String{},
									},
								},
							},
						},
					},
				},
			},
			"field-array-object-time": {
				Validator: &schema.Array{
					Values: schema.Field{
						Validator: &schema.Object{
							Schema: &schema.Schema{
								Fields: schema.Fields{
									"sub-time": {
										Validator: &testTime{},
									},
								},
							},
						},
					},
				},
			},
			"field-array-dict-string": {
				Validator: &schema.Array{
					Values: schema.Field{
						Validator: &schema.Dict{
							Values: schema.Field{
								Validator: &schema.String{},
							},
						},
					},
				},
			},
			"field-array-dict-time": {
				Validator: &schema.Array{
					Values: schema.Field{
						Validator: &schema.Dict{
							Values: schema.Field{
								Validator: &testTime{},
							},
						},
					},
				},
			},
		},
	}

	type tests struct {
		name    string
		payload map[string]any
		want    map[string]any
		wantErr bool
	}

	testCases := []tests{
		{
			name: "field-time",
			payload: map[string]any{
				"field-time": "2019-01-01T00:00:00Z",
			},
			want: map[string]any{
				"field-time": time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			wantErr: false,
		},
		{
			name: "field-time-object",
			payload: map[string]any{
				"field-time-object": map[string]any{
					"sub-time": "2019-01-01T00:00:00Z",
					"sub-object": map[string]any{
						"sub-sub-time": "2019-01-01T00:00:00Z",
					},
				},
			},
			want: map[string]any{
				"field-time-object": map[string]any{
					"sub-time": time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC),
					"sub-object": map[string]any{
						"sub-sub-time": time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC),
					},
				},
			},
			wantErr: false,
		},
		{
			name: "field-dict",
			payload: map[string]any{
				"field-dict": map[string]any{
					"dict-time": "2019-01-01T00:00:00Z",
				},
			},
			want: map[string]any{
				"field-dict": map[string]any{
					"dict-time": "2019-01-01T00:00:00Z",
				},
			},
			wantErr: false,
		},
		{
			name: "field-dict-time",
			payload: map[string]any{
				"field-dict-time": map[string]any{
					"dict-time": "2019-01-01T00:00:00Z",
				},
			},
			want: map[string]any{
				"field-dict-time": map[string]any{
					"dict-time": time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC),
				},
			},
			wantErr: false,
		},
		{
			name: "field-array-string",
			payload: map[string]any{
				"field-array-string": []any{"2019-01-01T00:00:00Z"},
			},
			want: map[string]any{
				"field-array-string": []any{"2019-01-01T00:00:00Z"},
			},
			wantErr: false,
		},
		{
			name: "field-array-object-string",
			payload: map[string]any{
				"field-array-object-string": []any{
					map[string]any{
						"sub-string": "2019-01-01T00:00:00Z",
					},
				},
			},
			want: map[string]any{
				"field-array-object-string": []any{
					map[string]any{
						"sub-string": "2019-01-01T00:00:00Z",
					},
				},
			},
		},
		{
			name: "field-array-time",
			payload: map[string]any{
				"field-array-time": []any{"2019-01-01T00:00:00Z"},
			},
			want: map[string]any{
				"field-array-time": []any{time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)},
			},
			wantErr: false,
		},
		{
			name: "field-array-object-time",
			payload: map[string]any{
				"field-array-object-time": []any{
					map[string]any{
						"sub-time": "2019-01-01T00:00:00Z",
					},
				},
			},
			want: map[string]any{
				"field-array-object-time": []any{
					map[string]any{
						"sub-time": time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC),
					},
				},
			},
			wantErr: false,
		},
		{
			name: "field-array-dict-string",
			payload: map[string]any{
				"field-array-dict-string": []any{
					map[string]any{
						"sub-string": "2019-01-01T00:00:00Z",
					},
				},
			},
			want: map[string]any{
				"field-array-dict-string": []any{
					map[string]any{
						"sub-string": "2019-01-01T00:00:00Z",
					},
				},
			},
		},
		{
			name: "field-array-dict-time",
			payload: map[string]any{
				"field-array-dict-time": []any{
					map[string]any{
						"sub-time": "2019-01-01T00:00:00Z",
					},
				},
			},
			want: map[string]any{
				"field-array-dict-time": []any{
					map[string]any{
						"sub-time": time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC),
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := clone.Clone(tc.payload)
			err := FixSchemaTypes(&testSchema, got)
			if err != nil {
				t.Errorf("evalSchema(%v) error = %v, wantErr %v", tc.payload, err, tc.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("evalSchema(%v) = %v, want %v", tc.payload, got, tc.want)
			}
		})
	}
}
