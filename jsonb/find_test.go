package jsonb

import (
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/lib/pq"
	"github.com/rs/rest-layer/schema"
	"github.com/rs/rest-layer/schema/query"
	"github.com/sanity-io/litter"
)

type String struct {
	schema.String
}

type Integer struct {
	schema.Integer
}

func (s Integer) PostgresType() string {
	if s.Boundaries == nil {
		return "INTEGER"
	}
	if s.Boundaries.Max == 0 || s.Boundaries.Max > 1<<31-1 {
		return "BIGINT"
	}
	if s.Boundaries.Max > 1<<15-1 {
		return "INTEGER"
	}
	return "SMALLINT"
}

type Decimal struct {
	schema.String
}

func (s Decimal) PostgresType() string {
	return "NUMERIC"
}

type Time struct {
	schema.Time
}

func (s Time) PostgresType() string {
	return "TIMESTAMP"
}

func Test_buildWheres(t *testing.T) {
	var addressSchema = schema.Schema{
		Fields: schema.Fields{
			"city": {
				Validator: &String{},
			},
			"state": {
				Validator: &Integer{},
			},
		},
	}

	var assetSchema = schema.Schema{
		Fields: schema.Fields{
			"age": {
				Validator: &Integer{},
			},
			"address": {
				Validator: &schema.Object{Schema: &addressSchema},
			},
		},
	}

	var testSchema = schema.Schema{
		Fields: schema.Fields{
			"date": {
				Validator: &Time{},
			},
			"name": {
				Validator: &schema.String{},
			},
			"age": {
				Validator: &schema.Integer{},
			},
			"weight": {
				Validator: &Decimal{},
			},
			"address": {
				Validator: &schema.Dict{},
			},
			"array": {
				Validator: &schema.Array{
					Values: schema.Field{
						Validator: &String{},
					},
				},
			},
			"array-with-typer": {
				Validator: &schema.Array{
					Values: schema.Field{
						Validator: &Integer{},
					},
				},
			},
			"asset": {
				Validator: &schema.Object{Schema: &assetSchema},
			},
			"object-array": {
				Validator: &schema.Array{
					Values: schema.Field{
						Validator: &schema.Object{Schema: &assetSchema},
					},
				},
			},
		},
	}

	type test struct {
		name       string
		query      query.Query
		want       []goqu.Expression
		wantSQL    string
		wantArgs   []interface{}
		wantErrStr string
	}

	tests := []test{
		{
			name: "query.And",
			query: query.Query{
				Predicate: query.Predicate{&query.And{
					&query.Equal{Field: "name", Value: "John"},
					&query.Equal{Field: "age", Value: 30},
				}},
			},
			want: []goqu.Expression{
				goqu.And(
					goqu.L("?->>?", goqu.C("payload"), goqu.V("name")).Eq("John"),
					goqu.L("?->>?", goqu.C("payload"), goqu.V("age")).Eq(30),
				),
			},
			wantSQL: `SELECT * FROM "table" WHERE (("payload"->>? = ?) AND ("payload"->>? = ?))`,
			wantArgs: []interface{}{
				"name",
				"John",
				"age",
				int64(30),
			},
		},
		{
			name: "query.And: nested",
			query: query.Query{
				Predicate: query.Predicate{
					&query.And{
						&query.Equal{Field: "address.city", Value: "John"},
						&query.Equal{Field: "address.state", Value: 30},
						&query.Equal{Field: "age", Value: 30},
					},
				},
			},
			want: []goqu.Expression{
				goqu.And(
					goqu.L("?->?->>?", goqu.C("payload"), goqu.V("address"), goqu.V("city")).Eq("John"),
					goqu.L("?->?->>?", goqu.C("payload"), goqu.V("address"), goqu.V("state")).Eq(30),
					goqu.L("?->>?", goqu.C("payload"), goqu.V("age")).Eq(30),
				),
			},
			wantSQL: `SELECT * FROM "table" WHERE (("payload"->?->>? = ?) AND ("payload"->?->>? = ?) AND ("payload"->>? = ?))`,
			wantArgs: []interface{}{
				"address",
				"city",
				"John",
				"address",
				"state",
				int64(30),
				"age",
				int64(30),
			},
		},
		{
			name: "query.Or",
			query: query.Query{
				Predicate: query.Predicate{&query.Or{
					&query.Equal{Field: "name", Value: "John"},
					&query.Equal{Field: "age", Value: 30},
				}},
			},
			want: []goqu.Expression{
				goqu.Or(
					goqu.L("?->>?", goqu.C("payload"), goqu.V("name")).Eq("John"),
					goqu.L("?->>?", goqu.C("payload"), goqu.V("age")).Eq(30),
				),
			},
			wantSQL: `SELECT * FROM "table" WHERE (("payload"->>? = ?) OR ("payload"->>? = ?))`,
			wantArgs: []interface{}{
				"name",
				"John",
				"age",
				int64(30),
			},
		},
		{
			name: "query.Or: nested",
			query: query.Query{
				Predicate: query.Predicate{
					&query.Or{
						&query.Equal{Field: "address.city", Value: "John"},
						&query.Equal{Field: "address.state", Value: 30},
						&query.Equal{Field: "age", Value: 30},
					},
				},
			},
			want: []goqu.Expression{
				goqu.Or(
					goqu.L("?->?->>?", goqu.C("payload"), goqu.V("address"), goqu.V("city")).Eq("John"),
					goqu.L("?->?->>?", goqu.C("payload"), goqu.V("address"), goqu.V("state")).Eq(30),
					goqu.L("?->>?", goqu.C("payload"), goqu.V("age")).Eq(30),
				),
			},
			wantSQL: `SELECT * FROM "table" WHERE (("payload"->?->>? = ?) OR ("payload"->?->>? = ?) OR ("payload"->>? = ?))`,
			wantArgs: []interface{}{
				"address",
				"city",
				"John",
				"address",
				"state",
				int64(30),
				"age",
				int64(30),
			},
		},
		{
			name: "query.In",
			query: query.Query{
				Predicate: query.Predicate{&query.In{Field: "name", Values: []interface{}{"John", "Jane"}}},
			},
			want: []goqu.Expression{
				goqu.L("?->>?", goqu.C("payload"), goqu.V("name")).In([]interface{}{"John", "Jane"}),
			},
			wantSQL: `SELECT * FROM "table" WHERE ("payload"->>? IN (?, ?))`,
			wantArgs: []interface{}{
				"name",
				"John",
				"Jane",
			},
		},
		{
			name: "query.In: nested",
			query: query.Query{
				Predicate: query.Predicate{&query.In{Field: "address.city", Values: []interface{}{"John", "Jane"}}},
			},
			want: []goqu.Expression{
				goqu.L("?->?->>?", goqu.C("payload"), goqu.V("address"), goqu.V("city")).In([]interface{}{"John", "Jane"}),
			},
			wantSQL: `SELECT * FROM "table" WHERE ("payload"->?->>? IN (?, ?))`,
			wantArgs: []interface{}{
				"address",
				"city",
				"John",
				"Jane",
			},
		},
		{
			name: "query.NotIn",
			query: query.Query{
				Predicate: query.Predicate{&query.NotIn{Field: "name", Values: []interface{}{"John", "Jane"}}},
			},
			want: []goqu.Expression{
				goqu.L("?->>?", goqu.C("payload"), goqu.V("name")).NotIn([]interface{}{"John", "Jane"}),
			},
			wantSQL: `SELECT * FROM "table" WHERE ("payload"->>? NOT IN (?, ?))`,
			wantArgs: []interface{}{
				"name",
				"John",
				"Jane",
			},
		},
		{
			name: "query.NotIn: nested",
			query: query.Query{
				Predicate: query.Predicate{&query.NotIn{Field: "address.city", Values: []interface{}{"John", "Jane"}}},
			},
			want: []goqu.Expression{
				goqu.L("?->?->>?", goqu.C("payload"), goqu.V("address"), goqu.V("city")).NotIn([]interface{}{"John", "Jane"}),
			},
			wantSQL: `SELECT * FROM "table" WHERE ("payload"->?->>? NOT IN (?, ?))`,
			wantArgs: []interface{}{
				"address",
				"city",
				"John",
				"Jane",
			},
		},
		{
			name: "query.Eq",
			query: query.Query{
				Predicate: query.Predicate{&query.Equal{Field: "name", Value: "John"}},
			},
			want: []goqu.Expression{
				goqu.L("?->>?", goqu.C("payload"), goqu.V("name")).Eq("John"),
			},
			wantSQL: `SELECT * FROM "table" WHERE ("payload"->>? = ?)`,
			wantArgs: []interface{}{
				"name",
				"John",
			},
		},
		{
			name: "query.Eq: nested",
			query: query.Query{
				Predicate: query.Predicate{&query.Equal{Field: "address.city", Value: "John"}},
			},
			want: []goqu.Expression{
				goqu.L("?->?->>?", goqu.C("payload"), goqu.V("address"), goqu.V("city")).Eq("John"),
			},
			wantSQL: `SELECT * FROM "table" WHERE ("payload"->?->>? = ?)`,
			wantArgs: []interface{}{
				"address",
				"city",
				"John",
			},
		},
		{
			name: "query.Eq: nested, 2 levels",
			query: query.Query{
				Predicate: query.Predicate{&query.Equal{Field: "asset.age", Value: "John"}},
			},
			want: []goqu.Expression{
				goqu.Cast(goqu.L("?->?->>?", goqu.C("payload"), goqu.V("asset"), goqu.V("age")), "INTEGER").Eq("John"),
			},
			wantSQL: `SELECT * FROM "table" WHERE (CAST("payload"->?->>? AS INTEGER) = ?)`,
			wantArgs: []interface{}{
				"asset",
				"age",
				"John",
			},
		},
		{
			name: "query.Eq: nested, 3 levels",
			query: query.Query{
				Predicate: query.Predicate{&query.Equal{Field: "asset.address.state", Value: 30}},
			},
			want: []goqu.Expression{
				goqu.Cast(goqu.L("?->?->?->>?", goqu.C("payload"), goqu.V("asset"), goqu.V("address"), goqu.V("state")), "INTEGER").Eq(30),
			},
			wantSQL: `SELECT * FROM "table" WHERE (CAST("payload"->?->?->>? AS INTEGER) = ?)`,
			wantArgs: []interface{}{
				"asset",
				"address",
				"state",
				int64(30),
			},
		},
		{
			name: "query.Eq: nested: multiple with AND",
			query: query.Query{
				Predicate: query.Predicate{&query.Equal{Field: "address.city", Value: "John"}, &query.Equal{Field: "address.state", Value: 30}},
			},
			want: []goqu.Expression{
				goqu.L("?->?->>?", goqu.C("payload"), goqu.V("address"), goqu.V("city")).Eq("John"),
				goqu.L("?->?->>?", goqu.C("payload"), goqu.V("address"), goqu.V("state")).Eq(30),
			},
			wantSQL: `SELECT * FROM "table" WHERE (("payload"->?->>? = ?) AND ("payload"->?->>? = ?))`,
			wantArgs: []interface{}{
				"address",
				"city",
				"John",
				"address",
				"state",
				int64(30),
			},
		},
		{
			name: "TODO: query.Eq: array",
			query: query.Query{
				Predicate: query.Predicate{&query.Equal{Field: "array", Value: []string{"John", "Doe"}}},
			},
			want: []goqu.Expression{
				goqu.L("? $$| ?", goqu.L("?->?", goqu.C("payload"), goqu.V("array")), pq.Array([]string{"John", "Doe"})),
			},
			wantSQL: `SELECT * FROM "table" WHERE "payload"->? ?| ?`,
			wantArgs: []interface{}{
				"array",
				`{"John","Doe"}`,
			},
		},
		{
			name: "TODO: query.Eq: array single",
			query: query.Query{
				Predicate: query.Predicate{&query.Equal{Field: "array-with-typer", Value: 1}},
			},
			want: []goqu.Expression{
				goqu.L("? $$| ?", goqu.L("?->?", goqu.C("payload"), goqu.V("array-with-typer")), pq.Array([]int{1})),
			},
			wantSQL: `SELECT * FROM "table" WHERE "payload"->? ?| ?`,
			wantArgs: []interface{}{
				"array-with-typer",
				`{1}`,
			},
		},
		{
			name: "query.NotEq",
			query: query.Query{
				Predicate: query.Predicate{&query.NotEqual{Field: "name", Value: "John"}},
			},
			want: []goqu.Expression{
				goqu.L("?->>?", goqu.C("payload"), goqu.V("name")).Neq("John"),
			},
			wantSQL: `SELECT * FROM "table" WHERE ("payload"->>? != ?)`,
			wantArgs: []interface{}{
				"name",
				"John",
			},
		},
		{
			name: "TODO: query.NotEq: array",
			query: query.Query{
				Predicate: query.Predicate{&query.NotEqual{Field: "array", Value: []string{"John", "Doe"}}},
			},
			want: []goqu.Expression{
				goqu.L("NOT (? $$| ?)", goqu.L("?->?", goqu.C("payload"), goqu.V("array")), pq.Array([]string{"John", "Doe"})),
			},
			wantSQL: `SELECT * FROM "table" WHERE NOT ("payload"->? ?| ?)`,
			wantArgs: []interface{}{
				"array",
				`{"John","Doe"}`,
			},
		},
		{
			name: "TODO: query.NotEq: array single",
			query: query.Query{
				Predicate: query.Predicate{&query.NotEqual{Field: "array-with-typer", Value: 1}},
			},
			want: []goqu.Expression{
				goqu.L("NOT (? $$| ?)", goqu.L("?->?", goqu.C("payload"), goqu.V("array-with-typer")), pq.Array([]int{1})),
			},
			wantSQL: `SELECT * FROM "table" WHERE NOT ("payload"->? ?| ?)`,
			wantArgs: []interface{}{
				"array-with-typer",
				`{1}`,
			},
		},
		{
			name: "query.GreaterThan",
			query: query.Query{
				Predicate: query.Predicate{&query.GreaterThan{Field: "age", Value: 30}},
			},
			want: []goqu.Expression{
				goqu.L("?->>?", goqu.C("payload"), goqu.V("age")).Gt(30),
			},
			wantSQL: `SELECT * FROM "table" WHERE ("payload"->>? > ?)`,
			wantArgs: []interface{}{
				"age",
				int64(30),
			},
		},
		{
			name: "query.GreaterThan: decimal",
			query: query.Query{
				Predicate: query.Predicate{&query.GreaterThan{Field: "weight", Value: 30.5}},
			},
			want: []goqu.Expression{
				goqu.Cast(goqu.L("?->>?", goqu.C("payload"), goqu.V("weight")), "NUMERIC").Gt(30.5),
			},
			wantSQL: `SELECT * FROM "table" WHERE (CAST("payload"->>? AS NUMERIC) > ?)`,
			wantArgs: []interface{}{
				"weight",
				30.5,
			},
		},
		{
			name: "query.GreaterThan: date",
			query: query.Query{
				Predicate: query.Predicate{&query.GreaterThan{Field: "date", Value: time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)}},
			},
			want: []goqu.Expression{
				goqu.Cast(goqu.L("?->>?", goqu.C("payload"), goqu.V("date")), "TIMESTAMP").Gt(time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)),
			},
			wantSQL: `SELECT * FROM "table" WHERE (CAST("payload"->>? AS TIMESTAMP) > ?)`,
			wantArgs: []interface{}{
				"date",
				time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "query.GreaterOrEqual",
			query: query.Query{
				Predicate: query.Predicate{&query.GreaterOrEqual{Field: "age", Value: 30}},
			},
			want: []goqu.Expression{
				goqu.L("?->>?", goqu.C("payload"), goqu.V("age")).Gte(30),
			},
			wantSQL: `SELECT * FROM "table" WHERE ("payload"->>? >= ?)`,
			wantArgs: []interface{}{
				"age",
				int64(30),
			},
		},
		{
			name: "query.LowerThan",
			query: query.Query{
				Predicate: query.Predicate{&query.LowerThan{Field: "age", Value: 30}},
			},
			want: []goqu.Expression{
				goqu.L("?->>?", goqu.C("payload"), goqu.V("age")).Lt(30),
			},
			wantSQL: `SELECT * FROM "table" WHERE ("payload"->>? < ?)`,
			wantArgs: []interface{}{
				"age",
				int64(30),
			},
		},
		{
			name: "query.LowerOrEqual",
			query: query.Query{
				Predicate: query.Predicate{&query.LowerOrEqual{Field: "age", Value: 30}},
			},
			want: []goqu.Expression{
				goqu.L("?->>?", goqu.C("payload"), goqu.V("age")).Lte(30),
			},
			wantSQL: `SELECT * FROM "table" WHERE ("payload"->>? <= ?)`,
			wantArgs: []interface{}{
				"age",
				int64(30),
			},
		},
		{
			name: "query.Regex",
			query: query.Query{
				Predicate: query.Predicate{&query.Regex{Field: "name", Value: regexp.MustCompile("John")}},
			},
			want: []goqu.Expression{
				goqu.L("?->>?", goqu.C("payload"), goqu.V("name")).RegexpLike("John"),
			},
			wantSQL: `SELECT * FROM "table" WHERE ("payload"->>? ~ ?)`,
			wantArgs: []interface{}{
				"name",
				"John",
			},
		},
		{
			name: "query.Regex: negated",
			query: query.Query{
				Predicate: query.Predicate{&query.Regex{Field: "name", Value: regexp.MustCompile("John"), Negated: true}},
			},
			want: []goqu.Expression{
				goqu.L("?->>?", goqu.C("payload"), goqu.V("name")).RegexpNotLike("John"),
			},
			wantSQL: `SELECT * FROM "table" WHERE ("payload"->>? !~ ?)`,
			wantArgs: []interface{}{
				"name",
				"John",
			},
		},
		{
			name: "query.Regex: case insensitive",
			query: query.Query{
				Predicate: query.Predicate{&query.Regex{Field: "name", Value: regexp.MustCompile("(i)John")}},
			},
			want: []goqu.Expression{
				goqu.L("?->>?", goqu.C("payload"), goqu.V("name")).RegexpILike("John"),
			},
			wantSQL: `SELECT * FROM "table" WHERE ("payload"->>? ~* ?)`,
			wantArgs: []interface{}{
				"name",
				"John",
			},
		},
		{
			name: "query.Regex: case insensitive, negated",
			query: query.Query{
				Predicate: query.Predicate{&query.Regex{Field: "name", Value: regexp.MustCompile("(i)John"), Negated: true}},
			},
			want: []goqu.Expression{
				goqu.L("?->>?", goqu.C("payload"), goqu.V("name")).RegexpNotILike("John"),
			},
			wantSQL: `SELECT * FROM "table" WHERE ("payload"->>? !~* ?)`,
			wantArgs: []interface{}{
				"name",
				"John",
			},
		},
		{
			name: "query.Regex: unknown flag",
			query: query.Query{
				Predicate: query.Predicate{&query.Regex{Field: "name", Value: regexp.MustCompile("(t)John")}},
			},
			want: []goqu.Expression{
				goqu.L("?->>?", goqu.C("payload"), goqu.V("name")).RegexpNotILike("John"),
			},
			wantErrStr: "unsupported regex flag t",
		},
		{
			name: "query.Exist",
			query: query.Query{
				Predicate: query.Predicate{&query.Exist{Field: "name"}},
			},
			want: []goqu.Expression{
				goqu.L("?->>?", goqu.C("payload"), goqu.V("name")).IsNotNull(),
			},
			wantSQL: `SELECT * FROM "table" WHERE ("payload"->>? IS NOT NULL)`,
			wantArgs: []interface{}{
				"name",
			},
		},
		{
			name: "query.Exist: nested",
			query: query.Query{
				Predicate: query.Predicate{&query.Exist{Field: "address.city"}},
			},
			want: []goqu.Expression{
				goqu.L("?->?->>?", goqu.C("payload"), goqu.V("address"), goqu.V("city")).IsNotNull(),
			},
			wantSQL: `SELECT * FROM "table" WHERE ("payload"->?->>? IS NOT NULL)`,
			wantArgs: []interface{}{
				"address",
				"city",
			},
		},
		{
			name: "query.NotExist",
			query: query.Query{
				Predicate: query.Predicate{&query.NotExist{Field: "name"}},
			},
			want: []goqu.Expression{
				goqu.L("?->>?", goqu.C("payload"), goqu.V("name")).IsNull(),
			},
			wantSQL: `SELECT * FROM "table" WHERE ("payload"->>? IS NULL)`,
			wantArgs: []interface{}{
				"name",
			},
		},
		{
			name: "query.NotExist: nested",
			query: query.Query{
				Predicate: query.Predicate{&query.NotExist{Field: "address.city"}},
			},
			want: []goqu.Expression{
				goqu.L("?->?->>?", goqu.C("payload"), goqu.V("address"), goqu.V("city")).IsNull(),
			},
			wantSQL: `SELECT * FROM "table" WHERE ("payload"->?->>? IS NULL)`,
			wantArgs: []interface{}{
				"address",
				"city",
			},
		},
		{
			name: "query.ElementMatch",
			query: query.Query{
				Predicate: query.Predicate{&query.ElemMatch{Field: "object-array", Exps: []query.Expression{&query.Equal{Field: "age", Value: 30}}}},
			},
			want: []goqu.Expression{
				goqu.Select("1").From(goqu.L("jsonb_array_elements(?) AS ?", goqu.L("?->>?", goqu.C("payload"), goqu.V("object-array")), goqu.C("object-array"))).Where(
					goqu.And(
						goqu.L("?->>?", goqu.C("object-array"), goqu.V("age")).Eq(30),
					),
				),
			},
			wantSQL: `SELECT * FROM "table" WHERE (SELECT "1" FROM jsonb_array_elements("payload"->>?) AS "object-array" WHERE ("object-array"->>? = ?))`,
			wantArgs: []interface{}{
				"object-array",
				"age",
				int64(30),
			},
		},
		{
			name: "query.ElementMatch: nested",
			query: query.Query{
				Predicate: query.Predicate{&query.ElemMatch{Field: "object-array", Exps: []query.Expression{&query.Equal{Field: "address.state", Value: 30}}}},
			},
			want: []goqu.Expression{
				goqu.Select("1").From(goqu.L("jsonb_array_elements(?) AS ?", goqu.L("?->>?", goqu.C("payload"), goqu.V("object-array")), goqu.C("object-array"))).Where(
					goqu.And(
						goqu.L("?->?->>?", goqu.C("object-array"), goqu.V("address"), goqu.V("state")).Eq(30),
					),
				),
			},
			wantSQL: `SELECT * FROM "table" WHERE (SELECT "1" FROM jsonb_array_elements("payload"->>?) AS "object-array" WHERE ("object-array"->?->>? = ?))`,
			wantArgs: []interface{}{
				"object-array",
				"address",
				"state",
				int64(30),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exprs, err := predicteToExpressions("", &testSchema, tt.query.Predicate)
			if err != nil {
				if err.Error() != tt.wantErrStr {
					t.Fatalf("Error = %s, want %s", err.Error(), tt.wantErrStr)
				} else {
					return
				}
			}

			if !reflect.DeepEqual(exprs, tt.want) {
				t.Errorf("Expressions = %+#v, want %+#v", exprs, tt.want)
				litter.Config.HidePrivateFields = false
				litter.Dump(exprs)
				litter.Dump(tt.want)
			}

			builder := goqu.From("table")
			buildWheres(&testSchema, &tt.query, builder)
			sql, args, _ := builder.Prepared(true).ToSQL()
			sql = strings.ReplaceAll(sql, "$$", "?")
			if sql != tt.wantSQL {
				t.Errorf("SQL = %+#v, want %+#v", sql, tt.wantSQL)
			}

			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("Args = %+#v, want %+#v", args, tt.wantArgs)
			}
		})
	}
}

func Test_buildSorts(t *testing.T) {
	var addressSchema = schema.Schema{
		Fields: schema.Fields{
			"city": {
				Validator: &String{},
			},
			"state": {
				Validator: &Integer{},
			},
		},
	}

	var testSchema = schema.Schema{
		Fields: schema.Fields{
			"date": {
				Validator: &Time{},
			},
			"name": {
				Validator: &schema.String{},
			},
			"age": {
				Validator: &schema.Integer{},
			},
			"weight": {
				Validator: &Decimal{},
			},
			"address": {
				Validator: &schema.Object{Schema: &addressSchema},
			},
		},
	}

	type test struct {
		name     string
		query    query.Query
		want     []goqu.Expression
		wantSQL  string
		wantArgs []interface{}
	}

	tests := []test{
		{
			name: "query.Sort",
			query: query.Query{
				Sort: []query.SortField{
					{Name: "name", Reversed: true},
					{Name: "age", Reversed: false},
				},
			},
			want: []goqu.Expression{
				goqu.C("name").Desc(),
				goqu.C("age").Asc(),
			},
			wantSQL: `SELECT * FROM "table" ORDER BY "payload"->>? DESC, "payload"->>? ASC`,
			wantArgs: []interface{}{
				"name",
				"age",
			},
		},
		{
			name: "query.Sort: nested",
			query: query.Query{
				Sort: []query.SortField{
					{Name: "address.city", Reversed: true},
					{Name: "age", Reversed: false},
				},
			},
			want: []goqu.Expression{
				goqu.L("?->?->>?", goqu.C("payload"), goqu.V("address"), goqu.V("city")).Desc(),
				goqu.C("age").Asc(),
			},
			wantSQL: `SELECT * FROM "table" ORDER BY "payload"->?->>? DESC, "payload"->>? ASC`,
			wantArgs: []interface{}{
				"address",
				"city",
				"age",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := goqu.From("table")
			buildSorts(&testSchema, &tt.query, builder)
			sql, args, _ := builder.Prepared(true).ToSQL()
			if sql != tt.wantSQL {
				t.Errorf("SQL = %+#v, want %+#v", sql, tt.wantSQL)
			}

			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("Args = %+#v, want %+#v", args, tt.wantArgs)
			}
		})
	}
}

func Test_buildSelects(t *testing.T) {
	type test struct {
		name     string
		query    query.Query
		wantSQL  string
		wantArgs []interface{}
	}

	tests := []test{
		{
			name: "query.Select",
			query: query.Query{
				Projection: []query.ProjectionField{
					{Name: "name"},
					{Name: "age"},
				},
			},
			wantSQL: `SELECT "id", "updated", "etag", jsonb_strip_nulls(jsonb_build_object('name', payload->?, 'age', payload->?)) AS "payload" FROM "table"`,
			wantArgs: []interface{}{
				"name",
				"age",
			},
		},
		{
			name: "query.Select: nested",
			query: query.Query{
				Projection: []query.ProjectionField{
					{Name: "address.city"},
					{Name: "age"},
				},
			},
			wantSQL: `SELECT "id", "updated", "etag", jsonb_strip_nulls(jsonb_build_object('address', payload->?, 'age', payload->?)) AS "payload" FROM "table"`,
			wantArgs: []interface{}{
				"address",
				"age",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := goqu.From("table")
			buildSelects(&tt.query, builder)
			sql, args, _ := builder.Prepared(true).ToSQL()
			if sql != tt.wantSQL {
				t.Errorf("SQL = %+#v, want %+#v", sql, tt.wantSQL)
			}

			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("Args = %+#v, want %+#v", args, tt.wantArgs)
			}
		})
	}
}
