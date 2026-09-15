package order

import (
	"context"
	"fmt"
	"time"
)

type Repository interface {
	Save(context context.Context, order Order) error
}

type EventPublisher interface {
	PublishCreated(ctx context.Context, order Order) error
}

type IDGenerator func() string

type Clock func() time.Time

type CreateInput struct {
	CustomerID    string
	AmountKopecks int64
}

type Service struct {
	repository Repository
	publisher  EventPublisher
	generateID IDGenerator
	now        Clock
}

func NewService(
	repository Repository,
	publisher EventPublisher,
	generateID IDGenerator,
	now Clock) *Service {
	return &Service{
		repository: repository,
		publisher:  publisher,
		generateID: generateID,
		now:        now,
	}
}

func (s *Service) Create(ctx context.Context, input CreateInput) (Order, error) {
	order, err := New(s.generateID(), input.CustomerID, input.AmountKopecks, s.now().UTC())

	if err != nil {
		return Order{}, err
	}

	if err := s.repository.Save(ctx, order); err != nil {
		return Order{}, fmt.Errorf("save order: %w", err)
	}

	if err := s.publisher.PublishCreated(ctx, order); err != nil {
		return Order{}, fmt.Errorf("publish order created event: %w", err)
	}

	return order, nil
}
