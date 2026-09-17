package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/inbox"
)

func TestProcessedEventRepositoryTryMarkProcessed(
	t *testing.T,
) {
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

	if _, err := tx.Exec(
		ctx,
		"DELETE FROM processed_events",
	); err != nil {
		t.Fatalf("clear processed events: %v", err)
	}

	repository := NewProcessedEventRepository(tx)

	message := inbox.Message{
		ConsumerGroup: "order-audit-v1",
		EventID:       "00000000-0000-4000-8000-000000000101",
		Topic:         "orders.created.v1",
		Partition:     2,
		Offset:        42,
	}

	first, err := repository.TryMarkProcessed(
		ctx,
		message,
	)
	if err != nil {
		t.Fatalf(
			"first TryMarkProcessed() unexpected error: %v",
			err,
		)
	}

	if !first {
		t.Fatal(
			"first TryMarkProcessed() = false, want true",
		)
	}

	second, err := repository.TryMarkProcessed(
		ctx,
		message,
	)
	if err != nil {
		t.Fatalf(
			"second TryMarkProcessed() unexpected error: %v",
			err,
		)
	}

	if second {
		t.Fatal(
			"second TryMarkProcessed() = true, want false",
		)
	}

	var (
		topic     string
		partition int32
		offset    int64
	)

	err = tx.QueryRow(
		ctx,
		`
			SELECT
				topic,
				partition,
				offset_value
			FROM processed_events
			WHERE consumer_group = $1
			  AND event_id = $2
		`,
		message.ConsumerGroup,
		message.EventID,
	).Scan(
		&topic,
		&partition,
		&offset,
	)
	if err != nil {
		t.Fatalf("select processed event: %v", err)
	}

	if topic != message.Topic {
		t.Errorf(
			"topic = %q, want %q",
			topic,
			message.Topic,
		)
	}

	if partition != message.Partition {
		t.Errorf(
			"partition = %d, want %d",
			partition,
			message.Partition,
		)
	}

	if offset != message.Offset {
		t.Errorf(
			"offset = %d, want %d",
			offset,
			message.Offset,
		)
	}
}
