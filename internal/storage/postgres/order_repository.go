package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	domainorder "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/order"
)

type DBTX interface {
	Exec(
		ctx context.Context,
		sql string,
		arguments ...any,
	) (pgconn.CommandTag, error)
}

type OrderRepository struct {
	db DBTX
}

func NewOrderRepository(db DBTX) *OrderRepository {
	return &OrderRepository{
		db: db,
	}
}

func (r *OrderRepository) Save(ctx context.Context, order domainorder.Order) error {
	const query = `
		INSERT INTO orders (
			id,
			customer_id,
			amount_kopecks,
			status,
			created_at
		)
		VALUES ($1, $2, $3, $4, $5)
	`

	_, err := r.db.Exec(
		ctx,
		query,
		order.ID,
		order.CustomerID,
		order.AmountKopecks,
		order.Status,
		order.CreatedAt,
	)

	if err != nil {
		return fmt.Errorf("insert order: %w", err)
	}

	return nil
}
