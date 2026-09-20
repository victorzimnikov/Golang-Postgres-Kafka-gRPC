package postgres

import "context"

type Builder struct {
	db  DBTX
	ctx context.Context
}

func NewBuilder(ctx context.Context, db DBTX) *Builder {
	return &Builder{
		ctx: ctx,
		db:  db,
	}
}
