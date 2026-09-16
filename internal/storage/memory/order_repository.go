package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"

	domainorder "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/order"
)

var (
	ErrOrderAlreadyExists = errors.New("order already exists")
	ErrEventAlreadyExists = errors.New("event already exists")
)

type OrderRepository struct {
	mu     sync.RWMutex
	orders map[string]domainorder.Order
	events map[string]domainorder.OrderCreatedEvent
}

func NewOrderRepository() *OrderRepository {
	return &OrderRepository{
		orders: make(map[string]domainorder.Order),
		events: make(map[string]domainorder.OrderCreatedEvent),
	}
}

func (r *OrderRepository) SaveWithEvent(
	ctx context.Context,
	order domainorder.Order,
	event domainorder.OrderCreatedEvent,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.orders[order.ID]; exists {
		return fmt.Errorf("%w: %s", ErrOrderAlreadyExists, order.ID)
	}

	if _, exists := r.events[event.EventID]; exists {
		return fmt.Errorf("%w: %s", ErrEventAlreadyExists, event.EventID)
	}

	r.orders[order.ID] = order
	r.events[event.EventID] = event

	return nil
}

func (r *OrderRepository) GetByID(
	ctx context.Context,
	id string,
) (domainorder.Order, error) {
	if err := ctx.Err(); err != nil {
		return domainorder.Order{}, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	order, exists := r.orders[id]
	if !exists {
		return domainorder.Order{}, domainorder.ErrOrderNotFound
	}

	return order, nil
}
