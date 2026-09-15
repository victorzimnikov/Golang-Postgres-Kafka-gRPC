package memory

import (
	"context"
	"errors"
	"testing"

	domainorder "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/order"
)

func TestOrderRepositorySave(t *testing.T) {
	repository := NewOrderRepository()

	order := domainorder.Order{
		ID: "order-1",
	}

	if err := repository.Save(context.Background(), order); err != nil {
		t.Fatalf("first Save() unexpected error: %v", err)
	}

	err := repository.Save(context.Background(), order)

	if !errors.Is(err, ErrOrderAlreadyExists) {
		t.Fatalf(
			"second Save() error = %v, want %v",
			err,
			ErrOrderAlreadyExists,
		)
	}
}

func TestOrderRepositorySaveReturnsContextError(t *testing.T) {
	repository := NewOrderRepository()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := repository.Save(
		ctx,
		domainorder.Order{
			ID: "order-1",
		},
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Save() error = %v, want %v", err, context.Canceled)
	}
}

func TestOrderRepositoryGetByID(t *testing.T) {
	repository := NewOrderRepository()

	want := domainorder.Order{
		ID:            "order-1",
		CustomerID:    "customer-1",
		AmountKopecks: 10_050,
		Status:        domainorder.StatusPending,
	}

	if err := repository.Save(
		context.Background(),
		want,
	); err != nil {
		t.Fatalf("Save() unexpected error: %v", err)
	}

	got, err := repository.GetByID(
		context.Background(),
		want.ID,
	)
	if err != nil {
		t.Fatalf("GetByID() unexpected error: %v", err)
	}

	if got != want {
		t.Errorf("GetByID() = %+v, want %+v", got, want)
	}
}

func TestOrderRepositoryGetByIDReturnsNotFound(t *testing.T) {
	repository := NewOrderRepository()

	_, err := repository.GetByID(
		context.Background(),
		"missing-order",
	)

	if !errors.Is(err, domainorder.ErrOrderNotFound) {
		t.Fatalf(
			"GetByID() error = %v, want %v",
			err,
			domainorder.ErrOrderNotFound,
		)
	}
}
