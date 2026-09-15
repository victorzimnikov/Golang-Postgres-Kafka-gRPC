package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"

	domainorder "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/order"
)

var ErrOrderAlreadyExists = errors.New("order already exists")

type OrderRepository struct {
	mu     sync.RWMutex
	orders map[string]domainorder.Order
}

func NewOrderRepository() *OrderRepository {
	return &OrderRepository{
		orders: make(map[string]domainorder.Order),
	}
}

func (r *OrderRepository) Save(
	ctx context.Context,
	order domainorder.Order,
) error {
	if err := ctx.Err(); err != nil {

		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.orders[order.ID]; exists {
		return fmt.Errorf("%w: %s", ErrOrderAlreadyExists, order.ID)
	}

	r.orders[order.ID] = order

	return nil
}

func (r *OrderRepository) GetByID(
	ctx context.Context,
	id string,
) (domainorder.Order, error) {
	if err := ctx.Err(); err != nil {
		return domainorder.Order{}, nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	order, exists := r.orders[id]
	if !exists {
		return domainorder.Order{}, domainorder.ErrOrderNotFound
	}

	return order, nil
}
