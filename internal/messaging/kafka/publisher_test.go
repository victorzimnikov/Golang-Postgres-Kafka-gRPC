package kafka

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
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

func TestOrderPublisherPublish(t *testing.T) {
	producer := &producerStub{}
	publisher := NewPublisher(
		producer,
		"orders.created.v1",
	)

	value := []byte(`{"event_id":"event-1"}`)

	if err := publisher.Publish(
		context.Background(),
		"order-1",
		value,
	); err != nil {
		t.Fatalf("Publish() unexpected error: %v", err)
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

	if string(record.Key) != "order-1" {
		t.Errorf(
			"record key = %q, want %q",
			record.Key,
			"order-1",
		)
	}

	if !bytes.Equal(record.Value, value) {
		t.Errorf(
			"record value = %s, want %s",
			record.Value,
			value,
		)
	}

	if !record.Timestamp.IsZero() {
		t.Errorf(
			"record timestamp = %v, want zero value",
			record.Timestamp,
		)
	}
}

func TestPublisherPublishReturnsProducerError(t *testing.T) {
	producerErr := errors.New("broker unavailable")

	producer := &producerStub{
		results: kgo.ProduceResults{
			{
				Record: &kgo.Record{},
				Err:    producerErr,
			},
		},
	}

	publisher := NewPublisher(
		producer,
		"orders.created.v1",
	)

	err := publisher.Publish(
		context.Background(),
		"order-1",
		[]byte(`{"event_id":"event-1"}`),
	)

	if !errors.Is(err, producerErr) {
		t.Fatalf(
			"Publish() error = %v, want wrapped %v",
			err,
			producerErr,
		)
	}
}
