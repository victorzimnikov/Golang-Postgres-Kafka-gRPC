package order

import (
	"context"
	"fmt"
	"time"
)

type Repository interface {
	SaveWithEvent(
		context context.Context,
		order Order,
		event OrderCreatedEvent,
	) error
	GetByID(context context.Context, id string) (Order, error)
}

type IDGenerator func() string

type Clock func() time.Time

type CreateInput struct {
	CustomerID    string
	AmountKopecks int64
}

type Service struct {
	repository Repository
	generateID IDGenerator
	now        Clock
}

func NewService(
	repository Repository,
	generateID IDGenerator,
	now Clock) *Service {
	return &Service{
		repository: repository,
		generateID: generateID,
		now:        now,
	}
}

func (s *Service) Create(
	ctx context.Context,
	input CreateInput,
) (Order, error) {
	order, err := New(s.generateID(), input.CustomerID, input.AmountKopecks, s.now().UTC())

	if err != nil {
		return Order{}, err
	}

	event := NewOrderCreatedEvent(order)

	if err := s.repository.SaveWithEvent(ctx, order, event); err != nil {
		return Order{}, fmt.Errorf("save order: %w", err)
	}

	return order, nil
}

func (s *Service) GetByID(
	ctx context.Context,
	id string,
) (Order, error) {
	if id == "" {
		return Order{}, ErrIDRequired
	}

	order, err := s.repository.GetByID(ctx, id)
	if err != nil {
		return Order{}, fmt.Errorf("get order by ID: %w", err)
	}

	return order, nil
}
