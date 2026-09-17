package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type SyncProducer interface {
	ProduceSync(
		ctx context.Context,
		records ...*kgo.Record,
	) kgo.ProduceResults
}

type Publisher struct {
	producer SyncProducer
	topic    string
}

func NewPublisher(
	producer SyncProducer,
	topic string,
) *Publisher {
	return &Publisher{
		producer: producer,
		topic:    topic,
	}
}

func (p *Publisher) Publish(
	ctx context.Context,
	key string,
	value []byte,
) error {
	record := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(key),
		Value: value,
	}

	if err := p.producer.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce kafka record: %w", err)
	}

	return nil
}
