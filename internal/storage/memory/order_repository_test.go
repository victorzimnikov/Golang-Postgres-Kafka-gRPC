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
