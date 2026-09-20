package postgres

import (
	"fmt"
)

type whereItem struct {
	operator string
	field    string
	arg      any

	group *whereCause
}

type whereCause struct {
	items []whereItem
}

func (w *whereCause) add(field string, arg any) {
	item := whereItem{
		field:    field,
		operator: "AND",
		arg:      arg,
	}

	w.items = append(w.items, item)
}

func (w *whereCause) addOr(where whereCause) {
	item := whereItem{
		operator: "OR",
		group:    &where,
	}

	w.items = append(w.items, item)
}

func (w *whereCause) build(ctx *buildContext) (string, error) {
	body, err := w.buildBody(ctx)
	if err != nil {
		return "", err
	}

	if body == "" {
		return "", nil
	}

	return "WHERE " + body, nil
}

func (w *whereCause) buildBody(ctx *buildContext) (string, error) {
	sql := ""

	for _, item := range w.items {
		var part string

		if item.group != nil {
			nested, err := item.group.buildBody(ctx)
			if err != nil {
				return "", err
			}

			if nested == "" {
				continue
			}

			part = "(" + nested + ")"
		} else {
			if item.arg == nil {
				part = quoteIdent(item.field) + " IS NULL"
			} else {
				part = quoteIdent(item.field) + " = " + ctx.bind(item.arg)
			}
		}

		if sql == "" {
			sql = part
			continue
		}

		switch item.operator {
		case "AND", "OR":
			sql = "(" + sql + " " + item.operator + " " + part + ")"
		default:
			return "", fmt.Errorf(
				"invalid logical operator: %q",
				item.operator,
			)
		}
	}

	return sql, nil
}
