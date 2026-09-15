package order

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestServiceGetByID(t *testing.T) {
	want := Order{
		ID:            "order-1",
		CustomerID:    "customer-1",
		AmountKopecks: 10_050,
		Status:        StatusPending,
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

	repository := &repositoryStub{
		getByIDFunc: func(
			_ context.Context,
			id string,
		) (Order, error) {
			if id != want.ID {
				t.Errorf("ID = %q, want %q", id, want.ID)
			}

			return want, nil
		},
	}

	service := NewService(
		repository,
		successfulPublisher(),
		func() string {
			return "unused"
		},
		time.Now,
	)

	got, err := service.GetByID(
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

func TestServiceGetByIDRejectsEmptyID(t *testing.T) {
	repositoryCalled := false

	repository := &repositoryStub{
		getByIDFunc: func(
			_ context.Context,
			_ string,
		) (Order, error) {
			repositoryCalled = true

			return Order{}, nil
		},
	}

	service := NewService(
		repository,
		successfulPublisher(),
		func() string {
			return "unused"
		},
		time.Now,
	)

	_, err := service.GetByID(
		context.Background(),
		"",
	)

	if !errors.Is(err, ErrIDRequired) {
		t.Fatalf(
			"GetByID() error = %v, want %v",
			err,
			ErrIDRequired,
		)
	}

	if repositoryCalled {
		t.Error("repository was called for an empty ID")
	}
}

func TestServiceGetByIDReturnsNotFound(t *testing.T) {
	repository := &repositoryStub{
		getByIDFunc: func(
			_ context.Context,
			_ string,
		) (Order, error) {
			return Order{}, ErrOrderNotFound
		},
	}

	service := NewService(
		repository,
		successfulPublisher(),
		func() string {
			return "unused"
		},
		time.Now,
	)

	_, err := service.GetByID(
		context.Background(),
		"missing-order",
	)

	if !errors.Is(err, ErrOrderNotFound) {
		t.Fatalf(
			"GetByID() error = %v, want wrapped %v",
			err,
			ErrOrderNotFound,
		)
	}
}
