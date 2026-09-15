package order

import (
	"errors"
	"time"
)

var (
	ErrIDRequired         = errors.New("order ID is required")
	ErrCustomerIDRequired = errors.New("customer ID is required")
	ErrAmountNotPositive  = errors.New("order amount must be positive")
	ErrOrderNotFound      = errors.New("order not found")
)

type Status string

const (
	StatusPending Status = "pending"
)

type Order struct {
	ID            string
	CustomerID    string
	AmountKopecks int64
	Status        Status
	CreatedAt     time.Time
}

func New(id string, customerID string, amountKopecks int64, createdAt time.Time) (Order, error) {
	switch {
	case id == "":
		return Order{}, ErrIDRequired
	case customerID == "":
		return Order{}, ErrCustomerIDRequired
	case amountKopecks <= 0:
		return Order{}, ErrAmountNotPositive
	}

	return Order{
		ID:            id,
		CustomerID:    customerID,
		AmountKopecks: amountKopecks,
		Status:        StatusPending,
		CreatedAt:     createdAt,
	}, nil
}
