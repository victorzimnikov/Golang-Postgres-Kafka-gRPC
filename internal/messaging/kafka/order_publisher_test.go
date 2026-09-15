package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	domainorder "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/order"
)

type producerStub struct {
	records []*kgo.Record
	results kgo.ProduceResults
}

func (s *producerStub) ProduceSync(
	_ context.Context,
	records ...*kgo.Record,
) kgo.ProduceResults {
	s.records = append(s.records, records...)

	return s.results
}

func TestOrderPublisherPublishCreated(t *testing.T) {
	producer := &producerStub{}
	publisher := NewOrderPublisher(
		producer,
		"orders.created.v1",
	)

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

	order := domainorder.Order{
		ID:            "00000000-0000-4000-8000-000000000001",
		CustomerID:    "customer-1",
		AmountKopecks: 10_050,
		Status:        domainorder.StatusPending,
		CreatedAt:     createdAt,
	}

	if err := publisher.PublishCreated(
		context.Background(),
		order,
	); err != nil {
		t.Fatalf("PublishCreated() unexpected error: %v", err)
	}

	if len(producer.records) != 1 {
		t.Fatalf(
			"produced records = %d, want 1",
			len(producer.records),
		)
	}

	record := producer.records[0]

	if record.Topic != "orders.created.v1" {
		t.Errorf(
			"record topic = %q, want %q",
			record.Topic,
			"orders.created.v1",
		)
	}

	if string(record.Key) != order.ID {
		t.Errorf(
			"record key = %q, want %q",
			record.Key,
			order.ID,
		)
	}

	var event OrderCreatedEvent

	if err := json.Unmarshal(record.Value, &event); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}

	if event.EventID != order.ID {
		t.Errorf(
			"EventID = %q, want %q",
			event.EventID,
			order.ID,
		)
	}

	if event.EventType != OrderCreatedEventType {
		t.Errorf(
			"EventType = %q, want %q",
			event.EventType,
			OrderCreatedEventType,
		)
	}

	if event.EventVersion != OrderCreatedEventVersion {
		t.Errorf(
			"EventVersion = %d, want %d",
			event.EventVersion,
			OrderCreatedEventVersion,
		)
	}

	if event.OrderID != order.ID {
		t.Errorf(
			"OrderID = %q, want %q",
			event.OrderID,
			order.ID,
		)
	}

	if !event.OccurredAt.Equal(order.CreatedAt) {
		t.Errorf(
			"OccurredAt = %v, want %v",
			event.OccurredAt,
			order.CreatedAt,
		)
	}
}

func TestOrderPublisherPublishCreatedReturnsProducerError(
	t *testing.T,
) {
	producerErr := errors.New("broker unavailable")

	producer := &producerStub{
		results: kgo.ProduceResults{
			{
				Record: &kgo.Record{},
				Err:    producerErr,
			},
		},
	}

	publisher := NewOrderPublisher(
		producer,
		"orders.created.v1",
	)

	err := publisher.PublishCreated(
		context.Background(),
		domainorder.Order{
			ID: "00000000-0000-4000-8000-000000000001",
		},
	)

	if !errors.Is(err, producerErr) {
		t.Fatalf(
			"PublishCreated() error = %v, want wrapped %v",
			err,
			producerErr,
		)
	}
}
