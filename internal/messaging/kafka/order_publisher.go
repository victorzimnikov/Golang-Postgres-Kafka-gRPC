package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	domainorder "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/order"
)

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

type SyncProducer interface {
	ProduceSync(
		ctx context.Context,
		records ...*kgo.Record,
	) kgo.ProduceResults
}

type OrderPublisher struct {
	producer SyncProducer
	topic    string
}

func NewOrderPublisher(
	producer SyncProducer,
	topic string,
) *OrderPublisher {
	return &OrderPublisher{
		producer: producer,
		topic:    topic,
	}
}

func (p *OrderPublisher) PublishCreated(
	ctx context.Context,
	order domainorder.Order,
) error {
	event := OrderCreatedEvent{
		EventID:       order.ID,
		EventType:     OrderCreatedEventType,
		EventVersion:  OrderCreatedEventVersion,
		OccurredAt:    order.CreatedAt,
		OrderID:       order.ID,
		CustomerID:    order.CustomerID,
		AmountKopecks: order.AmountKopecks,
		Status:        string(order.Status),
	}

	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal order created event: %w", err)
	}

	record := &kgo.Record{
		Topic:     p.topic,
		Key:       []byte(order.ID),
		Value:     value,
		Timestamp: order.CreatedAt,
	}

	if err := p.producer.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce order created event %w", err)
	}

	return nil
}
