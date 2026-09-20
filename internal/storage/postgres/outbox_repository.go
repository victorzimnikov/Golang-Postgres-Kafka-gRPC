package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/outbox"
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

	build := NewBuilder(ctx, tx)

	idField, err := CastField("id", TypeText)
	if err != nil {
		return false, err
	}

	aggregateIdField, err := CastField("aggregate_id", TypeText)
	if err != nil {
		return false, err
	}

	var event outbox.Event

	err = build.
		Select(
			"outbox_events",
			idField,
			aggregateIdField,
			Column("payload"),
			Column("occurred_at"),
		).
		WhereNull("published_at").
		OrderBy("occurred_at", OrderAsc).
		OrderBy("id", OrderAsc).
		Limit(1).
		Lock(LockModeUpdate).
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

	const markPublishedQuery = `
		UPDATE outbox_events
		SET
			published_at = clock_timestamp(),
			attempts = attempts + 1,
			last_error = NULL
		WHERE id = $1
	`

	if _, err := tx.Exec(ctx, markPublishedQuery, event.ID); err != nil {
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
	const query = `
		UPDATE outbox_events
		SET
			attempts = attempts + 1,
			last_error = $2
		WHERE id = $1
	`
	if _, err := tx.Exec(
		ctx,
		query,
		eventID,
		handlerErr.Error(),
	); err != nil {
		return fmt.Errorf("update failed outbox event: %w", err)
	}

	return nil
}
