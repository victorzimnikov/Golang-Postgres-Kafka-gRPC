package order

import (
	"context"
	"errors"
	"testing"
	"time"
)

type publisherStub struct {
	publishCreatedFunc func(
		ctx context.Context,
		order Order,
	) error
}

func (s *publisherStub) PublishCreated(
	ctx context.Context,
	order Order,
) error {
	return s.publishCreatedFunc(ctx, order)
}

func successfulPublisher() *publisherStub {
	return &publisherStub{
		publishCreatedFunc: func(
			_ context.Context,
			_ Order,
		) error {
			return nil
		},
	}
}

func TestServiceCreatePublishesCreatedOrder(t *testing.T) {
	createdAt := time.Date(
		2026,
		time.September,
		15,
		12,
		0,
		0,
		0,
		time.UTC,
	)

	repository := &repositoryStub{
		saveFunc: func(
			_ context.Context,
			_ Order,
		) error {
			return nil
		},
	}

	var publishedOrder Order

	publisher := &publisherStub{
		publishCreatedFunc: func(
			_ context.Context,
			order Order,
		) error {
			publishedOrder = order
			return nil
		},
	}

	service := NewService(
		repository,
		publisher,
		func() string {
			return "order-1"
		},
		func() time.Time {
			return createdAt
		},
	)

	got, err := service.Create(
		context.Background(),
		CreateInput{
			CustomerID:    "customer-1",
			AmountKopecks: 10_050,
		},
	)
	if err != nil {
		t.Fatalf("Create() unexpected error: %v", err)
	}

	if publishedOrder != got {
		t.Errorf(
			"published order = %+v, returned order = %+v",
			publishedOrder,
			got,
		)
	}
}

func TestServiceCreateReturnsPublisherError(t *testing.T) {
	publisherErr := errors.New("broker unavailable")

	repository := &repositoryStub{
		saveFunc: func(
			_ context.Context,
			_ Order,
		) error {
			return nil
		},
	}

	publisher := &publisherStub{
		publishCreatedFunc: func(
			_ context.Context,
			_ Order,
		) error {
			return publisherErr
		},
	}

	service := NewService(
		repository,
		publisher,
		func() string {
			return "order-1"
		},
		time.Now,
	)

	_, err := service.Create(
		context.Background(),
		CreateInput{
			CustomerID:    "customer-1",
			AmountKopecks: 10_050,
		},
	)

	if !errors.Is(err, publisherErr) {
		t.Fatalf(
			"Create() error = %v, want wrapped %v",
			err,
			publisherErr,
		)
	}
}
