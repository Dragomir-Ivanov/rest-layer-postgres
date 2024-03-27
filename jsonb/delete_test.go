package jsonb

import (
	"reflect"
	"testing"

	"github.com/doug-martin/goqu/v9"
	"github.com/rs/rest-layer/resource"
)

func Test_buildDelete(t *testing.T) {
	type test struct {
		name     string
		item     resource.Item
		want     goqu.Expression
		wantSQL  string
		wantArgs []interface{}
	}

	tests := []test{
		{
			name: "simple",
			item: resource.Item{
				ID:   "1",
				ETag: "ABCDEF",
			},
			want: goqu.And(
				goqu.L("id").Eq("1"),
				goqu.L("etag").Eq("ABCDEF"),
			),
			wantSQL:  `DELETE FROM "table" WHERE ((id = ?) AND (etag = ?))`,
			wantArgs: []interface{}{"1", "ABCDEF"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prepared := prepareDelete(&tt.item)
			if !reflect.DeepEqual(prepared, tt.want) {
				t.Errorf("prepareDelete() = %v, want %v", prepared, tt.want)
			}

			builder := goqu.Delete("table")
			buildDelete(&tt.item, builder)
			sql, args, err := builder.Prepared(true).ToSQL()
			if err != nil {
				t.Errorf("prepareDelete() error = %v", err)
				return
			}

			if sql != tt.wantSQL {
				t.Errorf("prepareDelete() = %v, want %v", sql, tt.wantSQL)
			}

			if len(args) != len(tt.wantArgs) {
				t.Errorf("prepareDelete() args = %v, want %v", args, tt.wantArgs)
			}

		})
	}
}
