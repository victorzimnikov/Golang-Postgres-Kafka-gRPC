package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/outbox"
)

func TestOutboxRepositoryProcessNext(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create connection pool: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping database: %v", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin test transaction: %v", err)
	}

	t.Cleanup(func() {
		_ = tx.Rollback(context.Background())
	})

	// Изолируем тест от ранее созданных pending-событий.
	if _, err := tx.Exec(ctx, "DELETE FROM outbox_events"); err != nil {
		t.Fatalf("clear outbox events: %v", err)
	}

	occurredAt := time.Date(
		2026,
		time.September,
		16,
		12,
		0,
		0,
		0,
		time.UTC,
	)

	const eventID = "00000000-0000-4000-8000-000000000101"
	const aggregateID = "00000000-0000-4000-8000-000000000001"

	_, err = tx.Exec(
		ctx,
		`
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
		`,
		eventID,
		"order",
		aggregateID,
		"order.created",
		1,
		[]byte(`{"event_id":"event-1"}`),
		occurredAt,
	)
	if err != nil {
		t.Fatalf("insert outbox event: %v", err)
	}

	repository := NewOutboxRepository(tx)

	var handled outbox.Event

	found, err := repository.ProcessNext(
		ctx,
		func(_ context.Context, event outbox.Event) error {
			handled = event
			return nil
		},
	)
	if err != nil {
		t.Fatalf("ProcessNext() unexpected error: %v", err)
	}

	if !found {
		t.Fatal("ProcessNext() processed = false, want true")
	}

	if handled.ID != eventID {
		t.Errorf(
			"handled event ID = %q, want %q",
			handled.ID,
			eventID,
		)
	}

	if handled.AggregateID != aggregateID {
		t.Errorf(
			"handled aggregate ID = %q, want %q",
			handled.AggregateID,
			aggregateID,
		)
	}

	if !handled.OccurredAt.Equal(occurredAt) {
		t.Errorf(
			"handled occurredAt = %v, want %v",
			handled.OccurredAt,
			occurredAt,
		)
	}

	var (
		published bool
		attempts  int
		lastError *string
	)

	err = tx.QueryRow(
		ctx,
		`
			SELECT
				published_at IS NOT NULL,
				attempts,
				last_error
			FROM outbox_events
			WHERE id = $1
		`,
		eventID,
	).Scan(
		&published,
		&attempts,
		&lastError,
	)
	if err != nil {
		t.Fatalf("select processed outbox event: %v", err)
	}

	if !published {
		t.Error("published_at is NULL after successful handler")
	}

	if attempts != 1 {
		t.Errorf("attempts = %d, want 1", attempts)
	}

	if lastError != nil {
		t.Errorf("last_error = %q, want NULL", *lastError)
	}
}

func TestOutboxRepositoryRecordsHandlerError(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create connection pool: %v", err)
	}
	t.Cleanup(pool.Close)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin test transaction: %v", err)
	}

	t.Cleanup(func() {
		_ = tx.Rollback(context.Background())
	})

	if _, err := tx.Exec(ctx, "DELETE FROM outbox_events"); err != nil {
		t.Fatalf("clear outbox events: %v", err)
	}

	const eventID = "00000000-0000-4000-8000-000000000102"

	_, err = tx.Exec(
		ctx,
		`
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
		`,
		eventID,
		"order",
		"00000000-0000-4000-8000-000000000002",
		"order.created",
		1,
		[]byte(`{"event_id":"event-2"}`),
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("insert outbox event: %v", err)
	}

	repository := NewOutboxRepository(tx)
	handlerErr := errors.New("Kafka unavailable")

	found, err := repository.ProcessNext(
		ctx,
		func(context.Context, outbox.Event) error {
			return handlerErr
		},
	)

	if !found {
		t.Fatal("ProcessNext() processed = false, want true")
	}

	if !errors.Is(err, handlerErr) {
		t.Fatalf(
			"ProcessNext() error = %v, want wrapped %v",
			err,
			handlerErr,
		)
	}

	var (
		published bool
		attempts  int
		lastError string
	)

	err = tx.QueryRow(
		ctx,
		`
			SELECT
				published_at IS NOT NULL,
				attempts,
				last_error
			FROM outbox_events
			WHERE id = $1
		`,
		eventID,
	).Scan(
		&published,
		&attempts,
		&lastError,
	)
	if err != nil {
		t.Fatalf("select failed outbox event: %v", err)
	}

	if published {
		t.Error("event was marked as published after handler error")
	}

	if attempts != 1 {
		t.Errorf("attempts = %d, want 1", attempts)
	}

	if lastError != handlerErr.Error() {
		t.Errorf(
			"last_error = %q, want %q",
			lastError,
			handlerErr,
		)
	}
}
