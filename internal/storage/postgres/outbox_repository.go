package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/outbox"
	"github.com/victorzimnikov/pgqb"
)

type OutboxRepository struct {
	db DBTX
}

func NewOutboxRepository(db DBTX) *OutboxRepository {
	return &OutboxRepository{
		db: db,
	}
}

func (r *OutboxRepository) ProcessNext(
	ctx context.Context,
	handler outbox.Handler,
) (bool, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	builder := pgqb.NewBuilder(ctx, tx)

	idField, err := pgqb.CastField("id", pgqb.TypeText)
	if err != nil {
		return false, err
	}

	aggregateIdField, err := pgqb.CastField("aggregate_id", pgqb.TypeText)
	if err != nil {
		return false, err
	}

	var event outbox.Event

	err = builder.
		Select(
			"outbox_events",
			idField,
			aggregateIdField,
			pgqb.Column("payload"),
			pgqb.Column("occurred_at"),
		).
		WhereNull("published_at").
		OrderBy("occurred_at", pgqb.OrderAsc).
		OrderBy("id", pgqb.OrderAsc).
		Limit(1).
		Lock(pgqb.LockModeUpdate).
		SkipLocked().
		Exec(
			&event.ID,
			&event.AggregateID,
			&event.Payload,
			&event.OccurredAt,
		)

	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("select outbox event: %w", err)
	}

	if err := handler(ctx, event); err != nil {
		if updateErr := recordFailure(ctx, tx, event.ID, err); updateErr != nil {
			return true, fmt.Errorf("record failure after handler error %v: %w", err, updateErr)
		}

		if commitErr := tx.Commit(ctx); commitErr != nil {
			return true, fmt.Errorf("commit failed delivery: %w", commitErr)
		}

		return true, fmt.Errorf("handle outbox event %s: %w", event.ID, err)
	}

	updateBuilder := builder.
		Update("outbox_events").
		SetExpr("published_at", pgqb.ClockTimestamp()).
		SetIncrement("attempts", 1).
		Set("last_error", nil).
		Where("id", event.ID)

	if _, err := updateBuilder.Exec(); err != nil {
		return true, fmt.Errorf("mark outbox event published: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return true, fmt.Errorf("commit published event: %w", err)
	}

	return true, nil
}

func recordFailure(
	ctx context.Context,
	tx pgx.Tx,
	eventID string,
	handlerErr error,
) error {
	updateBuilder := pgqb.
		NewBuilder(ctx, tx).
		Update("outbox_events").
		SetIncrement("attempts", 1).
		Set("last_error", handlerErr.Error()).
		Where("id", eventID)

	if _, err := updateBuilder.Exec(); err != nil {
		return fmt.Errorf("update failed outbox event: %w", err)
	}

	return nil
}
