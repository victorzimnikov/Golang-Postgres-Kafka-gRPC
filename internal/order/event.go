package order

import "time"

const (
	OrderCreatedEventType    = "order.created"
	OrderCreatedEventVersion = 1
)

type OrderCreatedEvent struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	EventVersion  int       `json:"event_version"`
	OccurredAt    time.Time `json:"occurred_at"`
	OrderID       string    `json:"order_id"`
	CustomerID    string    `json:"customer_id"`
	AmountKopecks int64     `json:"amount_kopecks"`
	Status        string    `json:"status"`
}

func NewOrderCreatedEvent(order Order) OrderCreatedEvent {
	return OrderCreatedEvent{
		EventID:       order.ID,
		EventType:     OrderCreatedEventType,
		EventVersion:  OrderCreatedEventVersion,
		OccurredAt:    order.CreatedAt,
		OrderID:       order.ID,
		CustomerID:    order.CustomerID,
		AmountKopecks: order.AmountKopecks,
		Status:        string(order.Status),
	}
}
