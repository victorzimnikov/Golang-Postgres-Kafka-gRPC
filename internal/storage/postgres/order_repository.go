package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	domainorder "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/order"
	"github.com/victorzimnikov/pgqb"
)

type DBTX interface {
	Exec(
		ctx context.Context,
		sql string,
		arguments ...any,
	) (pgconn.CommandTag, error)
	Begin(ctx context.Context) (pgx.Tx, error)
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

func (r *OrderRepository) SaveWithEvent(
	ctx context.Context,
	order domainorder.Order,
	event domainorder.OrderCreatedEvent,
) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event created event: %w", err)
	}

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	const insertOrderQuery = `
		INSERT INTO orders (
			id,
			customer_id,
			amount_kopecks,
			status,
			created_at
		)
		VALUES ($1, $2, $3, $4, $5)
	`

	_, err = tx.Exec(
		ctx,
		insertOrderQuery,
		order.ID,
		order.CustomerID,
		order.AmountKopecks,
		order.Status,
		order.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert order: %w", err)
	}

	const insertEventQuery = `
		INSERT INTO outbox_events (
			id,
			aggregate_type,
			aggregate_id,
			event_type,
			event_version,
			payload,
			occurred_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`

	_, err = tx.Exec(
		ctx,
		insertEventQuery,
		event.EventID,
		"order",
		order.ID,
		event.EventType,
		event.EventVersion,
		payload,
		event.OccurredAt,
	)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (r *OrderRepository) GetByID(
	ctx context.Context,
	id string,
) (domainorder.Order, error) {
	var (
		order  domainorder.Order
		status string
	)

	idField, err := pgqb.CastField("id", pgqb.TypeText)
	if err != nil {
		return domainorder.Order{}, err
	}

	err = pgqb.
		NewBuilder(ctx, r.db).
		Select(
			"orders",
			idField,
			pgqb.Column("customer_id"),
			pgqb.Column("amount_kopecks"),
			pgqb.Column("status"),
			pgqb.Column("created_at"),
		).
		Where("id", id).
		Exec(
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
