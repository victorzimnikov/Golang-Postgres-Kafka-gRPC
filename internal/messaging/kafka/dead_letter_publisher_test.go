package kafka

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDeadLetterPublisherPublish(t *testing.T) {
	producer := &producerStub{}
	publisher := NewDeadLetterPublisher(
		producer,
		"orders.created.v1.dlq",
	)

	sourceRecord := &kgo.Record{
		Topic:     "orders.created.v1",
		Partition: 2,
		Offset:    42,
		Key:       []byte("order-1"),
		Value:     []byte(`{"event_id":""}`),
		Headers: []kgo.RecordHeader{
			{
				Key:   "trace-id",
				Value: []byte("trace-1"),
			},
		},
	}

	cause := errors.New("event ID is required")

	if err := publisher.Publish(
		context.Background(),
		sourceRecord,
		cause,
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

	if record.Topic != "orders.created.v1.dlq" {
		t.Errorf(
			"record topic = %q, want %q",
			record.Topic,
			"orders.created.v1.dlq",
		)
	}

	if !bytes.Equal(record.Key, sourceRecord.Key) {
		t.Errorf(
			"record key = %q, want %q",
			record.Key,
			sourceRecord.Key,
		)
	}

	if !bytes.Equal(record.Value, sourceRecord.Value) {
		t.Errorf(
			"record value = %s, want %s",
			record.Value,
			sourceRecord.Value,
		)
	}

	headers := make(map[string]string, len(record.Headers))
	for _, header := range record.Headers {
		headers[header.Key] = string(header.Value)
	}

	wantHeaders := map[string]string{
		"trace-id":               "trace-1",
		"dlq-original-topic":     "orders.created.v1",
		"dlq-original-partition": "2",
		"dlq-original-offset":    "42",
		"dlq-error":              "event ID is required",
	}

	for key, want := range wantHeaders {
		if got := headers[key]; got != want {
			t.Errorf(
				"header %q = %q, want %q",
				key,
				got,
				want,
			)
		}
	}

	if !record.Timestamp.IsZero() {
		t.Errorf(
			"record timestamp = %v, want zero value",
			record.Timestamp,
		)
	}
}

func TestDeadLetterPublisherPublishReturnsProducerError(t *testing.T) {
	producerErr := errors.New("broker unavailable")

	producer := &producerStub{
		results: kgo.ProduceResults{
			{
				Record: &kgo.Record{},
				Err:    producerErr,
			},
		},
	}

	publisher := NewDeadLetterPublisher(
		producer,
		"orders.created.v1.dlq",
	)

	err := publisher.Publish(
		context.Background(),
		&kgo.Record{
			Topic: "orders.created.v1",
		},
		errors.New("invalid message"),
	)

	if !errors.Is(err, producerErr) {
		t.Fatalf(
			"Publish() error = %v, want wrapped %v",
			err,
			producerErr,
		)
	}
}

func TestDeadLetterPublisherPublishRejectsInvalidArguments(t *testing.T) {
	tests := []struct {
		name         string
		sourceRecord *kgo.Record
		cause        error
	}{
		{
			name:         "nil source record",
			sourceRecord: nil,
			cause:        errors.New("invalid message"),
		},
		{
			name:         "nil cause",
			sourceRecord: &kgo.Record{},
			cause:        nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			producer := &producerStub{}
			publisher := NewDeadLetterPublisher(
				producer,
				"orders.created.v1.dlq",
			)

			err := publisher.Publish(
				context.Background(),
				test.sourceRecord,
				test.cause,
			)
			if err == nil {
				t.Fatal("Publish() error = nil, want non-nil")
			}

			if len(producer.records) != 0 {
				t.Errorf(
					"produced records = %d, want 0",
					len(producer.records),
				)
			}
		})
	}
}
