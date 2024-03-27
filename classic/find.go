package pgsql

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/Dragomir-Ivanov/rest-layer-postgres/internal"
	"github.com/doug-martin/goqu/v9"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema"
	"github.com/rs/rest-layer/schema/query"

	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	"github.com/doug-martin/goqu/v9/exp"
)

func (s store) Find(ctx context.Context, q *query.Query) (*resource.ItemList, error) {
	builder := s.dialect.From(s.table)
	buildSelects(q, builder)
	buildWheres(s.schema, q, builder)
	buildSorts(q, builder)
	buildPagination(q, builder)

	sqlStr, args, err := builder.Prepared(true).ToSQL()
	if err != nil {
		return nil, err
	}

	slog.DebugContext(ctx, "pgsql.Find", "sql", sqlStr, "args", args)

	rows, err := s.db.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("sql: %s args: %v", sqlStr, args))
	}
	defer rows.Close()

	// result mapping
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	limit := 10
	if q.Window != nil {
		limit = q.Window.Limit
	}

	result := &resource.ItemList{
		Total: -1,
		Limit: limit,
		Items: []*resource.Item{},
	}
	for rows.Next() {
		rowMap := make(map[string]any)
		rowVals := make([]any, len(cols))
		rowValPtrs := make([]any, len(cols))
		var etag string
		var updated time.Time

		for i := range cols {
			rowValPtrs[i] = &rowVals[i]
		}

		err := rows.Scan(rowValPtrs...)
		if err != nil {
			return nil, err
		}

		for i, v := range rowVals {
			// Skip null values
			if v == nil {
				continue
			}

			b, ok := v.([]byte)
			if ok {
				v = string(b)
			}

			switch cols[i] {
			case "_etag":
				etag = v.(string)
			case "_updated":
				updated = v.(time.Time)
			default:
				rowMap[cols[i]] = v
			}
		}

		// Converting itemID from int64 to int
		itemID := rowMap["id"]
		switch t := itemID.(type) {
		case int64:
			itemID = strconv.Itoa(int(t))
		}

		// Converting json string to json node
		for name, field := range s.jsonFields {
			if c, ok := rowMap[name]; ok {
				if jsonStr, ok := c.(string); ok {
					rowMap[name] = toJsonNode(&field, jsonStr)
				}
			}
		}

		item := &resource.Item{
			ID:      itemID,
			ETag:    etag,
			Updated: updated,
			Payload: rowMap,
		}
		internal.FixSchemaTypes(s.schema, item.Payload)
		result.Items = append(result.Items, item)
	}

	return result, nil
}

func (s store) Count(ctx context.Context, q *query.Query) (int, error) {
	builder := s.dialect.From(s.table).Select(goqu.COUNT(goqu.Star()))
	buildWheres(s.schema, q, builder)

	sqlStr, args, err := builder.Prepared(true).ToSQL()
	if err != nil {
		return 0, err
	}

	slog.DebugContext(ctx, "pgsql.Count", "sql", sqlStr, "args", args)

	row := s.db.QueryRowContext(ctx, sqlStr, args...)

	var count int
	err = row.Scan(&count)

	return count, err
}

func toJsonNode(field *schema.Field, cell string) any {
	switch field.Validator.(type) {
	case *schema.Object, *schema.Dict, nil:
		jsonNode := make(map[string]any)
		if err := json.Unmarshal([]byte(cell), &jsonNode); err != nil {
			return nil
		}
		return jsonNode
	case *schema.Array:
		jsonNode := make([]any, 0)
		if err := json.Unmarshal([]byte(cell), &jsonNode); err != nil {
			return nil
		}
		return jsonNode
	}
	return nil
}

func buildPagination(q *query.Query, builder *goqu.SelectDataset) {
	limit := 20
	offset := 0

	window := q.Window
	if window != nil {
		limit = window.Limit
		offset = window.Offset
	}
	*builder = *builder.Limit(uint(limit))
	*builder = *builder.Offset(uint(offset))
}

func buildSorts(q *query.Query, builder *goqu.SelectDataset) {
	for _, field := range q.Sort {
		if field.Reversed {
			*builder = *builder.Order(goqu.C(field.Name).Desc())
		} else {
			*builder = *builder.Order(goqu.C(field.Name).Asc())
		}
	}
}

func buildWheres(s *schema.Schema, q *query.Query, builder *goqu.SelectDataset) {
	expressions := predicteToExpressions(s, q.Predicate)
	*builder = *builder.Where(expressions...)
}

func buildSelects(q *query.Query, builder *goqu.SelectDataset) {
	pj := q.Projection
	if len(pj) == 0 || hasStar(pj) {
		*builder = *builder.Select(goqu.Star())
		return
	}

	selectFields := make([]any, 0, len(pj))
	for _, field := range pj {
		if len(field.Alias) > 0 {
			selectFields = append(selectFields, goqu.I(field.Name).As(field.Alias), goqu.I(field.Name))
		} else {
			selectFields = append(selectFields, goqu.I(field.Name))
		}
	}

	*builder = *builder.Select(selectFields...)
}

func hasStar(pj query.Projection) bool {
	return match(pj, func(pf query.ProjectionField) bool {
		return pf.Name == "*"
	})
}

func match(pj query.Projection, predicate func(pf query.ProjectionField) bool) bool {
	for _, field := range pj {
		if predicate(field) {
			return true
		}
	}
	return false
}

func predicteToExpressions(s *schema.Schema, q query.Predicate) (expressions []goqu.Expression) {
	for _, e := range q {
		switch t := e.(type) {
		case *query.And:
			for _, subExp := range *t {
				expressions = append(expressions, goqu.And(predicteToExpressions(s, query.Predicate{subExp})...))
			}
		case *query.Or:
			for _, subExp := range *t {
				expressions = append(expressions, goqu.Or(predicteToExpressions(s, query.Predicate{subExp})...))
			}
		case *query.In:
			expressions = append(expressions, postgresJsonbSupport(t.Field, false).In(t.Values))
		case *query.NotIn:
			expressions = append(expressions, postgresJsonbSupport(t.Field, false).NotIn(t.Values))

		case *query.Equal:
			isSchemaArray := false
			ff := s.GetField(t.Field)
			if ff != nil {
				if _, ok := ff.Validator.(*schema.Array); ok {
					isSchemaArray = true
				}
			}
			var expr exp.Expression
			if isSchemaArray {
				expr = goqu.L("? \\?\\| ?", postgresJsonbSupport(t.Field, false), pq.Array(convertToArray(t.Value)))
			} else {
				expr = postgresJsonbSupport(t.Field, false).Eq(t.Value)
			}
			expressions = append(expressions, expr)

		// case *query.Equal:
		// 	expressions = append(expressions, goqu.C(postgresJsonbSupport(t.Field)).Eq(t.Value))
		case *query.NotEqual:
			expressions = append(expressions, postgresJsonbSupport(t.Field, false).Neq(t.Value))
		case *query.GreaterThan:
			expressions = append(expressions, postgresJsonbSupport(t.Field, false).Gt(t.Value))
		case *query.GreaterOrEqual:
			expressions = append(expressions, postgresJsonbSupport(t.Field, false).Gte(t.Value))
		case *query.LowerThan:
			expressions = append(expressions, postgresJsonbSupport(t.Field, false).Lt(t.Value))
		case *query.LowerOrEqual:
			expressions = append(expressions, postgresJsonbSupport(t.Field, false).Lte(t.Value))
		case *query.Regex:
			expressions = append(expressions, postgresJsonbSupport(t.Field, false).RegexpLike(t.Value))
		case *query.Exist:
			expr := postgresJsonbSupport(t.Field, false).IsNotNull()
			expressions = append(expressions, expr)
		case *query.NotExist:
			expr := postgresJsonbSupport(t.Field, false).IsNull()
			expressions = append(expressions, expr)
		default:
			fmt.Printf("%T\n", t)
			slog.Warn("not supported predicate. ignored")
		}
	}
	return
}

func postgresJsonbSupport(field string, asJSOBN bool) exp.LiteralExpression {
	if !strings.Contains(field, ".") {
		return goqu.L("?", goqu.C(field))
	}

	if field == "id" {
		return goqu.L(field)
	}

	var exprs []any
	strs := strings.Split(field, ".")
	literalText := ""
	for i, str := range strs {
		if i == len(strs)-1 {
			if asJSOBN {
				literalText += "->"
			} else {
				literalText += "->>"
			}
		} else if i != 0 {
			literalText += "->"
		}
		literalText += "?"

		if i == 0 {
			exprs = append(exprs, goqu.C(str))
		} else {
			exprs = append(exprs, goqu.V(str))
		}
	}

	return goqu.L(literalText, exprs...)
}

func convertToArray(input interface{}) interface{} {
	// Check if input is array using reflect
	if reflect.TypeOf(input).Kind() == reflect.Array || reflect.TypeOf(input).Kind() == reflect.Slice {
		return input
	}

	// Create an array of the input type
	arrayType := reflect.SliceOf(reflect.TypeOf(input))
	array := reflect.MakeSlice(arrayType, 1, 1)
	array.Index(0).Set(reflect.ValueOf(input))

	return array.Interface()
}
