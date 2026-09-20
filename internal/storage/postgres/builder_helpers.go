package postgres

import "strings"

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

type SelectExpr struct {
	sql string
}

func Column(name string) SelectExpr {
	return SelectExpr{
		sql: quoteIdent(name),
	}
}

func CastField(field string, fieldTypes ...FieldType) (SelectExpr, error) {
	result := quoteIdent(field)

	for _, fieldType := range fieldTypes {
		typeSql, err := fieldType.sql()
		if err != nil {
			return SelectExpr{}, err
		}

		result += "::" + typeSql
	}

	return SelectExpr{sql: result}, nil
}
