package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	domainorder "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/order"
)

func TestOrderRepositorySave(t *testing.T) {
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
		t.Fatalf("begin transaction: %v", err)
	}
	t.Cleanup(func() {
		_ = tx.Rollback(context.Background())
	})

	want := domainorder.Order{
		ID:            "00000000-0000-4000-8000-000000000001",
		CustomerID:    "customer-1",
		AmountKopecks: 10_050,
		Status:        domainorder.StatusPending,
		CreatedAt: time.Date(
			2026,
			time.September,
			15,
			12,
			0,
			0,
			0,
			time.UTC,
		),
	}

	repository := NewOrderRepository(tx)

	if err := repository.Save(ctx, want); err != nil {
		t.Fatalf("Save() unexpected error: %v", err)
	}

	got, err := repository.GetByID(ctx, want.ID)
	if err != nil {
		t.Fatalf("GetByID() unexpected error: %v", err)
	}

	if got.ID != want.ID {
		t.Errorf("ID = %q, want %q", got.ID, want.ID)
	}

	if got.CustomerID != want.CustomerID {
		t.Errorf(
			"CustomerID = %q, want %q",
			got.CustomerID,
			want.CustomerID,
		)
	}

	if got.AmountKopecks != want.AmountKopecks {
		t.Errorf(
			"AmountKopecks = %d, want %d",
			got.AmountKopecks,
			want.AmountKopecks,
		)
	}

	if got.Status != want.Status {
		t.Errorf(
			"Status = %q, want %q",
			got.Status,
			want.Status,
		)
	}

	if !got.CreatedAt.Equal(want.CreatedAt) {
		t.Errorf(
			"CreatedAt = %v, want %v",
			got.CreatedAt,
			want.CreatedAt,
		)
	}

	_, err = repository.GetByID(
		ctx,
		"00000000-0000-4000-8000-000000000002",
	)

	if !errors.Is(err, domainorder.ErrOrderNotFound) {
		t.Fatalf(
			"GetByID() error = %v, want %v",
			err,
			domainorder.ErrOrderNotFound,
		)
	}
}
