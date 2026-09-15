package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	domainorder "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/order"
)

type DBTX interface {
	Exec(
		ctx context.Context,
		sql string,
		arguments ...any,
	) (pgconn.CommandTag, error)
	QueryRow(
		ctx context.Context,
		sql string,
		arguments ...any,
	) pgx.Row
}

type OrderRepository struct {
	db DBTX
}

func NewOrderRepository(db DBTX) *OrderRepository {
	return &OrderRepository{
		db: db,
	}
}

func (r *OrderRepository) Save(
	ctx context.Context,
	order domainorder.Order,
) error {
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

func (r *OrderRepository) GetByID(
	ctx context.Context,
	id string,
) (domainorder.Order, error) {
	const query = `
		SELECT
			id::text,
			customer_id,
			amount_kopecks,
			status,
			created_at
		FROM orders
		WHERE id = $1
	`

	var (
		order  domainorder.Order
		status string
	)

	err := r.db.QueryRow(ctx, query, id).Scan(
		&order.ID,
		&order.CustomerID,
		&order.AmountKopecks,
		&status,
		&order.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domainorder.Order{}, domainorder.ErrOrderNotFound
	}

	if err != nil {
		return domainorder.Order{}, fmt.Errorf(
			"select order by ID: %w",
			err,
		)
	}

	order.Status = domainorder.Status(status)

	return order, nil
}
