package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	domainorder "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/order"
)

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
	event := domainorder.NewOrderCreatedEvent(order)

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
