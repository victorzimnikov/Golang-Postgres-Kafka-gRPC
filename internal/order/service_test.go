package order

import (
	"context"
	"errors"
	"testing"
	"time"
)

type repositoryStub struct {
	saveWithEventFunc func(
		ctx context.Context,
		order Order,
		event OrderCreatedEvent,
	) error
	getByIDFunc func(ctx context.Context, id string) (Order, error)
}

func (r *repositoryStub) SaveWithEvent(
	ctx context.Context,
	order Order,
	event OrderCreatedEvent,
) error {
	return r.saveWithEventFunc(ctx, order, event)
}

func (r *repositoryStub) GetByID(ctx context.Context, id string) (Order, error) {
	return r.getByIDFunc(ctx, id)
}

func TestServiceCreate(t *testing.T) {
	createdAt := time.Date(2026, time.September, 15, 12, 0, 0, 0, time.UTC)

	var (
		savedOrder Order
		savedEvent OrderCreatedEvent
	)

	repository := &repositoryStub{
		saveWithEventFunc: func(
			_ context.Context,
			order Order,
			event OrderCreatedEvent,
		) error {
			savedOrder = order
			savedEvent = event
			return nil
		},
	}

	service := NewService(
		repository,
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

	wantEvent := NewOrderCreatedEvent(got)

	if savedEvent != wantEvent {
		t.Errorf(
			"saved event = %+v, want %+v",
			savedEvent,
			wantEvent,
		)
	}

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
		saveWithEventFunc: func(
			_ context.Context,
			order Order,
			_event OrderCreatedEvent,
		) error {
			saveCalled = true
			return nil
		},
	}

	service := NewService(
		repository,
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
		saveWithEventFunc: func(
			ctx context.Context,
			order Order,
			_event OrderCreatedEvent,
		) error {
			return repositoryErr
		},
	}

	service := NewService(
		repository,
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
