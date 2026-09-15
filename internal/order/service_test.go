package order

import (
	"context"
	"errors"
	"testing"
	"time"
)

type repositoryStub struct {
	saveFunc func(ctx context.Context, order Order) error
}

func (r *repositoryStub) Save(ctx context.Context, order Order) error {
	return r.saveFunc(ctx, order)
}

func TestServiceCreate(t *testing.T) {
	createdAt := time.Date(2026, time.September, 15, 12, 0, 0, 0, time.UTC)

	var savedOrder Order

	repository := &repositoryStub{
		saveFunc: func(_ context.Context, order Order) error {
			savedOrder = order
			return nil
		},
	}

	service := NewService(
		repository,
		successfulPublisher(),
		func() string { return "order-1" },
		func() time.Time { return createdAt },
	)

	got, err := service.Create(
		context.Background(),
		CreateInput{
			CustomerID:    "customer-1",
			AmountKopecks: 10_500,
		},
	)

	if err != nil {
		t.Fatalf("Create() unexpected error: %v", err)
	}

	if got != savedOrder {
		t.Errorf("saved order = %+v, returned order %+v", savedOrder, got)
	}

	if got.ID != "order-1" {
		t.Errorf("ID = %q, want %q", got.ID, "order-1")
	}

	if got.CreatedAt != createdAt {
		t.Errorf("CreatedAt = %v, want %v", got.CreatedAt, createdAt)
	}
}

func TestServiceCreateDoesNotSaveInvalidOrder(t *testing.T) {
	saveCalled := false

	repository := &repositoryStub{
		saveFunc: func(_ context.Context, order Order) error {
			saveCalled = true
			return nil
		},
	}

	service := NewService(
		repository,
		successfulPublisher(),
		func() string { return "order-1" },
		time.Now,
	)

	_, err := service.Create(
		context.Background(),
		CreateInput{
			CustomerID:    "customer-1",
			AmountKopecks: 0,
		},
	)

	if !errors.Is(err, ErrAmountNotPositive) {
		t.Fatalf("Create() error = %v, want %v", err, ErrAmountNotPositive)
	}

	if saveCalled {
		t.Errorf("repository Save() was called for an invalid order")
	}
}

func TestServiceCreateReturnsRepositoryError(t *testing.T) {
	repositoryErr := errors.New("repository unavailable")

	repository := &repositoryStub{
		saveFunc: func(ctx context.Context, order Order) error {
			return repositoryErr
		},
	}

	service := NewService(
		repository,
		successfulPublisher(),
		func() string { return "order-1" },
		time.Now,
	)

	_, err := service.Create(
		context.Background(),
		CreateInput{
			CustomerID:    "customer-1",
			AmountKopecks: 10_500,
		},
	)

	if !errors.Is(err, repositoryErr) {
		t.Fatalf("Create() error = %v, want wrapped %v", err, repositoryErr)
	}
}
