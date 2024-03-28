package jsonb

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema"
	"github.com/rs/rest-layer/schema/query"

	"github.com/Dragomir-Ivanov/rest-layer-postgres/internal"
)

func (s store) Find(ctx context.Context, q *query.Query) (*resource.ItemList, error) {
	builder := s.dialect.From(s.table)
	buildSelects(q, builder)
	err := buildWheres(s.schema, q, builder)
	if err != nil {
		return nil, errors.Wrapf(err, "predicate: %v", q.Predicate)
	}

	buildSorts(s.schema, q, builder)
	buildPagination(q, builder)

	sqlStr, args, err := builder.Prepared(true).ToSQL()
	if err != nil {
		return nil, errors.Wrapf(err, "predicate: %v", q.Predicate)
	}
	sqlStr = strings.ReplaceAll(sqlStr, "$$", "?")

	slog.DebugContext(ctx, "pgsql.Find", "sql", sqlStr, "args", args)

	rows, err := s.db.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
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
		rowVals := make([]any, len(cols))
		rowValPtrs := make([]any, len(cols))

		for i := range cols {
			rowValPtrs[i] = &rowVals[i]
		}

		err := rows.Scan(rowValPtrs...)
		if err != nil {
			return nil, err
		}

		var ID any
		var etag string
		var updated time.Time
		payload := make(map[string]any)

		for i, v := range rowVals {
			b, ok := v.([]byte)
			if ok {
				v = string(b)
			}

			switch cols[i] {
			case "id":
				ID = v
				switch t := v.(type) {
				case int64:
					ID = strconv.Itoa(int(t))
				}
			case "etag":
				etag = v.(string)
			case "updated":
				updated = v.(time.Time)
			case "payload":
				if err := json.Unmarshal([]byte(v.(string)), &payload); err != nil {
					return nil, err
				}
			}
		}

		payload["id"] = ID

		item := &resource.Item{
			ID:      ID,
			ETag:    etag,
			Updated: updated,
			Payload: payload,
		}
		internal.FixSchemaTypes(s.schema, item.Payload)
		result.Items = append(result.Items, item)
	}

	return result, nil
}

func (s store) Count(ctx context.Context, q *query.Query) (int, error) {
	builder := s.dialect.From(s.table).Select(goqu.COUNT(goqu.Star()))

	err := buildWheres(s.schema, q, builder)
	if err != nil {
		return 0, err
	}

	sqlStr, args, err := builder.Prepared(true).ToSQL()
	if err != nil {
		return 0, err
	}

	slog.DebugContext(ctx, "pqsql.Count", "sql", sqlStr, "args", args)

	row := s.db.QueryRowContext(ctx, sqlStr, args...)

	var count int
	err = row.Scan(&count)

	return count, err
}

func buildSelects(q *query.Query, builder *goqu.SelectDataset) {
	pj := q.Projection
	if len(pj) == 0 || hasStar(pj) {
		*builder = *builder.Select(goqu.Star())
		return
	}

	prefixExprs := []any{goqu.C("id"), goqu.C("updated"), goqu.C("etag")}
	var exprs []any
	for _, field := range pj {
		if field.Name == "id" {
			continue
		}

		// Extract only top level field name
		name := field.Name
		fname := strings.Split(field.Name, ".")
		if len(fname) > 1 {
			name = fname[0]
		}
		exprs = append(exprs, goqu.L("'"+name+"'"), goqu.L(`payload->?`, goqu.V(name)))
	}

	prefixExprs = append(prefixExprs,
		goqu.L("jsonb_strip_nulls(jsonb_build_object?)", exprs).As("payload"),
	)

	*builder = *builder.Select(prefixExprs...)
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

func buildWheres(s *schema.Schema, q *query.Query, builder *goqu.SelectDataset) error {
	expressions, err := predicteToExpressions("", s, q.Predicate)
	if err != nil {
		return err
	}
	*builder = *builder.Where(expressions...)
	return nil
}

func predicteToExpressions(parent string, s *schema.Schema, q query.Predicate) (expressions []exp.Expression, err error) {
	for _, e := range q {
		switch t := e.(type) {

		case *query.And:
			var exprs []exp.Expression
			for _, subExp := range *t {
				var sube []exp.Expression
				sube, err = predicteToExpressions(parent, s, query.Predicate{subExp})
				if err != nil {
					return nil, err
				}
				exprs = append(exprs, sube...)
			}
			expressions = append(expressions, goqu.And(exprs...))

		case *query.Or:
			var exprs []exp.Expression
			for _, subExp := range *t {
				var sube []exp.Expression
				sube, err = predicteToExpressions(parent, s, query.Predicate{subExp})
				if err != nil {
					return nil, err
				}
				exprs = append(exprs, sube...)
			}
			expressions = append(expressions, goqu.Or(exprs...))

		case *query.In:
			expressions = append(expressions, postgresJsonbSupport(parent, t.Field, false).In(t.Values))

		case *query.NotIn:
			expressions = append(expressions, postgresJsonbSupport(parent, t.Field, false).NotIn(t.Values))

		case *query.Equal:
			isSchemaArray := false
			ff := s.GetField(parent + t.Field)
			if ff != nil {
				if _, ok := ff.Validator.(*schema.Array); ok {
					isSchemaArray = true
				}
			}
			pgtype := internal.PgtypeFromField(s, parent, t.Field)
			var expr exp.Expression
			if pgtype != "" {
				if isSchemaArray {
					expr = goqu.L("? $$| ?", goqu.Cast(postgresJsonbSupport(parent, t.Field, true), pgtype), pq.Array(convertToArray(t.Value)))
				} else {
					expr = goqu.Cast(postgresJsonbSupport(parent, t.Field, false), pgtype).Eq(t.Value)
				}
			} else {
				if isSchemaArray {
					expr = goqu.L("? $$| ?", postgresJsonbSupport(parent, t.Field, true), pq.Array(convertToArray(t.Value)))
				} else {
					expr = postgresJsonbSupport(parent, t.Field, false).Eq(t.Value)
				}
			}
			expressions = append(expressions, expr)

		case *query.NotEqual:
			isSchemaArray := false
			ff := s.GetField(parent + t.Field)
			if ff != nil {
				if _, ok := ff.Validator.(*schema.Array); ok {
					isSchemaArray = true
				}
			}
			pgtype := internal.PgtypeFromField(s, parent, t.Field)
			var expr exp.Expression
			if pgtype != "" {
				if isSchemaArray {
					expr = goqu.L("NOT (? $$| ?)", goqu.Cast(postgresJsonbSupport(parent, t.Field, true), pgtype), pq.Array(convertToArray(t.Value)))
				} else {
					expr = goqu.Cast(postgresJsonbSupport(parent, t.Field, false), pgtype).Neq(t.Value)
				}
			} else {
				if isSchemaArray {
					expr = goqu.L("NOT (? $$| ?)", postgresJsonbSupport(parent, t.Field, true), pq.Array(convertToArray(t.Value)))
				} else {
					expr = postgresJsonbSupport(parent, t.Field, false).Neq(t.Value)
				}
			}
			expressions = append(expressions, expr)

		case *query.GreaterThan:
			pgtype := internal.PgtypeFromField(s, parent, t.Field)
			var expr exp.Expression
			if pgtype != "" {
				expr = goqu.Cast(postgresJsonbSupport(parent, t.Field, false), pgtype).Gt(t.Value)
			} else {
				expr = postgresJsonbSupport(parent, t.Field, false).Gt(t.Value)
			}
			expressions = append(expressions, expr)

		case *query.GreaterOrEqual:
			pgtype := internal.PgtypeFromField(s, parent, t.Field)
			var expr exp.Expression
			if pgtype != "" {
				expr = goqu.Cast(postgresJsonbSupport(parent, t.Field, false), pgtype).Gte(t.Value)
			} else {
				expr = postgresJsonbSupport(parent, t.Field, false).Gte(t.Value)
			}
			expressions = append(expressions, expr)

		case *query.LowerThan:
			pgtype := internal.PgtypeFromField(s, parent, t.Field)
			var expr exp.Expression
			if pgtype != "" {
				expr = goqu.Cast(postgresJsonbSupport(parent, t.Field, false), pgtype).Lt(t.Value)
			} else {
				expr = postgresJsonbSupport(parent, t.Field, false).Lt(t.Value)
			}
			expressions = append(expressions, expr)

		case *query.LowerOrEqual:
			pgtype := internal.PgtypeFromField(s, parent, t.Field)
			var expr exp.Expression
			if pgtype != "" {
				expr = goqu.Cast(postgresJsonbSupport(parent, t.Field, false), pgtype).Lte(t.Value)
			} else {
				expr = postgresJsonbSupport(parent, t.Field, false).Lte(t.Value)
			}
			expressions = append(expressions, expr)

		case *query.Regex:
			re := regexp.MustCompile(`(\(.*\))?(.*)`)
			matches := re.FindStringSubmatch(t.Value.String())
			caseInsensitive := false
			value := t.Value.String()
			if len(matches) == 3 {
				flags := matches[1]
				flags = strings.ReplaceAll(flags, "(", "")
				flags = strings.ReplaceAll(flags, ")", "")
				for _, c := range flags {
					switch c {
					case 'i':
						caseInsensitive = true
					case 'm':
					case 's':
					default:
						return nil, fmt.Errorf("unsupported regex flag %c", c)
					}
				}

				value = matches[2]
			}
			pgtype := internal.PgtypeFromField(s, parent, t.Field)
			var expr exp.Expression
			if pgtype != "" {
				if t.Negated {
					if caseInsensitive {
						expr = goqu.Cast(postgresJsonbSupport(parent, t.Field, false), pgtype).RegexpNotILike(value)
					} else {
						expr = goqu.Cast(postgresJsonbSupport(parent, t.Field, false), pgtype).RegexpNotLike(value)
					}
				} else {
					if caseInsensitive {
						expr = goqu.Cast(postgresJsonbSupport(parent, t.Field, false), pgtype).RegexpILike(value)
					} else {
						expr = goqu.Cast(postgresJsonbSupport(parent, t.Field, false), pgtype).RegexpLike(value)
					}
				}
			} else {
				if t.Negated {
					if caseInsensitive {
						expr = postgresJsonbSupport(parent, t.Field, false).RegexpNotILike(value)
					} else {
						expr = postgresJsonbSupport(parent, t.Field, false).RegexpNotLike(value)
					}
				} else {
					if caseInsensitive {
						expr = postgresJsonbSupport(parent, t.Field, false).RegexpILike(value)
					} else {
						expr = postgresJsonbSupport(parent, t.Field, false).RegexpLike(value)
					}
				}
			}
			expressions = append(expressions, expr)

		case *query.Exist:
			expr := postgresJsonbSupport(parent, t.Field, false).IsNotNull()
			expressions = append(expressions, expr)

		case *query.NotExist:
			expr := postgresJsonbSupport(parent, t.Field, false).IsNull()
			expressions = append(expressions, expr)

		case *query.ElemMatch:
			exprs := make([]exp.Expression, 0)
			for _, p := range t.Exps {
				var sube []exp.Expression
				sube, err = predicteToExpressions(t.Field, s, query.Predicate{p})
				if err != nil {
					return nil, err
				}
				exprs = append(exprs, goqu.And(sube...))
			}
			ss := goqu.Select("1").From(goqu.L("jsonb_array_elements(?) AS ?", postgresJsonbSupport(parent, t.Field, false), goqu.C(t.Field))).Where(exprs...)
			expressions = append(expressions, ss)

		default:
			return nil, fmt.Errorf("unsupported predicate %T", t)
		}
	}

	return
}

func postgresJsonbSupport(parent, field string, asJSOBN bool) exp.LiteralExpression {
	if field == "id" {
		return goqu.L(field)
	}

	if parent == "" {
		parent = "payload."
	} else {
		parent += "."
	}
	field = parent + field

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

func prepareSorts(s *schema.Schema, q *query.Query) (orders []exp.OrderedExpression) {
	for _, field := range q.Sort {
		expr := goqu.L(field.Name)
		switch field.Name {
		case "id":
		case "etag":
		case "created":
		default:
			expr = postgresJsonbSupport("", field.Name, false)
		}

		pgtype := internal.PgtypeFromField(s, "", field.Name)
		if field.Reversed {
			if pgtype != "" {
				orders = append(orders, exp.OrderedExpression(goqu.Cast(expr, pgtype).Desc()))
			} else {
				orders = append(orders, exp.OrderedExpression(expr.Desc()))
			}
		} else {
			if pgtype != "" {
				orders = append(orders, exp.OrderedExpression(goqu.Cast(expr, pgtype).Asc()))
			} else {
				orders = append(orders, exp.OrderedExpression(expr.Asc()))
			}
		}
	}
	return
}

func buildSorts(s *schema.Schema, q *query.Query, builder *goqu.SelectDataset) {
	orders := prepareSorts(s, q)
	if len(orders) > 0 {
		*builder = *builder.Order(orders...)
	}
}

func preparePagination(q *query.Query) (limit, offset int) {
	limit = 20
	offset = 0

	window := q.Window
	if window != nil {
		limit = window.Limit
		offset = window.Offset
	}
	return
}

func buildPagination(q *query.Query, builder *goqu.SelectDataset) {
	limit, offset := preparePagination(q)
	*builder = *builder.Limit(uint(limit))
	*builder = *builder.Offset(uint(offset))
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
