package postgres

import (
	"fmt"
	"strings"
)

type buildContext struct {
	args []any
}

func (c *buildContext) bind(value any) string {
	c.args = append(c.args, value)

	return fmt.Sprintf("$%d", len(c.args))
}

type SelectBuilder struct {
	Builder

	table   string
	limit   int
	offset  int
	where   whereCause
	fields  []SelectExpr
	orderBy []string

	lockTables        []string
	lockMode          string
	lockModeBehaviour string

	err error
}

func (b *Builder) Select(table string, fields ...SelectExpr) *SelectBuilder {
	return &SelectBuilder{
		table:   table,
		fields:  fields,
		Builder: *b,
	}
}

func (b *SelectBuilder) Where(field string, arg any) *SelectBuilder {
	b.where.add(field, arg)

	return b
}

func (b *SelectBuilder) WhereNull(field string) *SelectBuilder {
	b.where.add(field, nil)

	return b
}

func (b *SelectBuilder) WhereOr(build func(*SelectBuilder)) *SelectBuilder {
	group := new(SelectBuilder)
	build(group)

	b.where.addOr(group.where)

	return b
}

func (b *SelectBuilder) Limit(limit int) *SelectBuilder {
	b.limit = limit

	return b
}

func (b *SelectBuilder) Offset(offset int) *SelectBuilder {
	b.offset = offset

	return b
}

func (b *SelectBuilder) OrderBy(field string, direction OrderDirection) *SelectBuilder {
	if b.err != nil {
		return b
	}

	directionSql, err := direction.sql()
	if err != nil {
		b.err = fmt.Errorf(
			"order by %q: %w",
			field,
			err,
		)

		return b
	}

	b.orderBy = append(b.orderBy, quoteIdent(field)+" "+directionSql)

	return b
}

func (b *SelectBuilder) Lock(
	mode LockMode,
) *SelectBuilder {
	if b.err != nil {
		return b
	}

	modeSql, err := mode.sql()
	if err != nil {
		b.err = fmt.Errorf(
			"lock mode %d: %w",
			mode,
			err,
		)

		return b
	}

	b.lockMode = modeSql

	return b
}

func (b *SelectBuilder) LockTables(tables ...string) *SelectBuilder {
	if b.err != nil {
		return b
	}

	if b.lockMode == "" {
		b.err = fmt.Errorf(
			"lock mode not set",
		)

		return b
	}

	b.lockTables = tables

	return b
}

func (b *SelectBuilder) SkipLocked() *SelectBuilder {
	if b.err != nil {
		return b
	}

	if b.lockMode == "" {
		b.err = fmt.Errorf(
			"lock mode not set",
		)

		return b
	}

	if b.lockModeBehaviour != "" {
		b.err = fmt.Errorf("lock behaviour is already set: %s", b.lockModeBehaviour)

		return b
	}

	b.lockModeBehaviour = "SKIP LOCKED"

	return b
}

func (b *SelectBuilder) NoWait() *SelectBuilder {
	if b.err != nil {
		return b
	}

	if b.lockMode == "" {
		b.err = fmt.Errorf(
			"lock mode not set",
		)

		return b
	}

	if b.lockModeBehaviour != "" {
		b.err = fmt.Errorf("lock behaviour is already set: %s", b.lockModeBehaviour)

		return b
	}

	b.lockModeBehaviour = "NOWAIT"

	return b
}

func (b *SelectBuilder) Exec(dest ...any) error {
	if b.err != nil {
		return b.err
	}

	buildCtx := &buildContext{}

	fields := "*"

	if len(b.fields) > 0 {
		for _, field := range b.fields {
			if fields == "*" {
				fields = field.sql
			} else {
				fields += ", " + field.sql
			}
		}
	}

	query := fmt.Sprintf("SELECT %s FROM %s", fields, quoteIdent(b.table))

	whereSQL, err := b.where.build(buildCtx)
	if err != nil {
		return err
	}

	if whereSQL != "" {
		query += " " + whereSQL
	}

	if len(b.orderBy) > 0 {
		query = query + " ORDER BY " + strings.Join(b.orderBy, ", ")
	}

	if b.limit != 0 {
		query = fmt.Sprintf("%s LIMIT %s", query, buildCtx.bind(b.limit))
	}

	if b.offset != 0 {
		query = fmt.Sprintf("%s OFFSET %s", query, buildCtx.bind(b.offset))
	}

	if b.lockMode != "" {
		if len(b.lockTables) > 0 {
			tables := ""

			for _, tableName := range b.lockTables {
				if tables == "" {
					tables += quoteIdent(tableName)
				} else {
					tables += ", " + quoteIdent(tableName)
				}
			}

			query = fmt.Sprintf("%s FOR %s OF %s", query, b.lockMode, tables)
		} else {
			query = fmt.Sprintf("%s FOR %s", query, b.lockMode)
		}

		if b.lockModeBehaviour != "" {
			query = fmt.Sprintf("%s %s", query, b.lockModeBehaviour)
		}

	}

	return b.db.QueryRow(b.ctx, query, buildCtx.args...).Scan(dest...)
}
