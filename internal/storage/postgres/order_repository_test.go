package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	domainorder "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/order"
	"github.com/victorzimnikov/pgqb"
)

func TestOrderRepositorySaveWithEvent(t *testing.T) {
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

	// Внешняя транзакция изолирует тест:
	// все его данные будут удалены через Rollback.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin test transaction: %v", err)
	}

	t.Cleanup(func() {
		_ = tx.Rollback(context.Background())
	})

	repository := NewOrderRepository(tx)

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

	wantEvent := domainorder.NewOrderCreatedEvent(want)

	if err := repository.SaveWithEvent(
		ctx,
		want,
		wantEvent,
	); err != nil {
		t.Fatalf("SaveWithEvent() unexpected error: %v", err)
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

	var payload []byte

	err = pgqb.
		NewBuilder(ctx, tx).
		Select("outbox_events", pgqb.Column("payload")).
		Where("id", wantEvent.EventID).
		Exec(&payload)
	if err != nil {
		t.Fatalf("select outbox event: %v", err)
	}

	var gotEvent domainorder.OrderCreatedEvent

	if err := json.Unmarshal(payload, &gotEvent); err != nil {
		t.Fatalf("unmarshal outbox event: %v", err)
	}

	if gotEvent != wantEvent {
		t.Errorf(
			"outbox event = %+v, want %+v",
			gotEvent,
			wantEvent,
		)
	}

	// Проверяем атомарность. Сначала будет вставлен заказ,
	// затем вставка события завершится ошибкой из-за версии 0.
	rollbackOrder := domainorder.Order{
		ID:            "00000000-0000-4000-8000-000000000002",
		CustomerID:    "customer-2",
		AmountKopecks: 20_000,
		Status:        domainorder.StatusPending,
		CreatedAt:     want.CreatedAt,
	}

	invalidEvent := domainorder.NewOrderCreatedEvent(rollbackOrder)
	invalidEvent.EventVersion = 0

	err = repository.SaveWithEvent(
		ctx,
		rollbackOrder,
		invalidEvent,
	)
	if err == nil {
		t.Fatal("SaveWithEvent() expected outbox insert error")
	}

	// Если транзакция работает правильно, вставленный перед ошибкой
	// заказ также должен быть отменён.
	_, err = repository.GetByID(ctx, rollbackOrder.ID)

	if !errors.Is(err, domainorder.ErrOrderNotFound) {
		t.Fatalf(
			"GetByID() after rollback error = %v, want %v",
			err,
			domainorder.ErrOrderNotFound,
		)
	}
}
