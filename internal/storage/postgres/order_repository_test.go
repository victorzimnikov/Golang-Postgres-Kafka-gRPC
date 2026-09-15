package postgres

import (
	"context"
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
		gotID            string
		gotCustomerID    string
		gotAmountKopecks int64
		gotStatus        string
		gotCreatedAt     time.Time
	)

	err = tx.QueryRow(ctx, query, want.ID).Scan(
		&gotID,
		&gotCustomerID,
		&gotAmountKopecks,
		&gotStatus,
		&gotCreatedAt,
	)
	if err != nil {
		t.Fatalf("query saved order: %v", err)
	}

	if gotID != want.ID {
		t.Errorf("ID = %q, want %q", gotID, want.ID)
	}

	if gotCustomerID != want.CustomerID {
		t.Errorf(
			"CustomerID = %q, want %q",
			gotCustomerID,
			want.CustomerID,
		)
	}

	if gotAmountKopecks != want.AmountKopecks {
		t.Errorf(
			"AmountKopecks = %d, want %d",
			gotAmountKopecks,
			want.AmountKopecks,
		)
	}

	if gotStatus != string(want.Status) {
		t.Errorf(
			"Status = %q, want %q",
			gotStatus,
			want.Status,
		)
	}

	if !gotCreatedAt.Equal(want.CreatedAt) {
		t.Errorf(
			"CreatedAt = %v, want %v",
			gotCreatedAt,
			want.CreatedAt,
		)
	}
}
