package outbox

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

type repositoryStub struct {
	event  Event
	exists bool
}

func (s *repositoryStub) ProcessNext(
	ctx context.Context,
	handler Handler,
) (bool, error) {
	if !s.exists {
		return false, nil
	}

	return true, handler(ctx, s.event)
}

type publisherStub struct {
	key    string
	value  []byte
	err    error
	called bool
}

func (s *publisherStub) Publish(
	_ context.Context,
	key string,
	value []byte,
) error {
	s.called = true
	s.key = key
	s.value = value

	return s.err
}

func TestProcessorProcessNext(t *testing.T) {
	occurredAt := time.Date(
		2026,
		time.September,
		16,
		12,
		0,
		0,
		0,
		time.UTC,
	)

	event := Event{
		ID:          "event-1",
		AggregateID: "order-1",
		Payload:     []byte(`{"event_id":"event-1"}`),
		OccurredAt:  occurredAt,
	}

	repository := &repositoryStub{
		event:  event,
		exists: true,
	}

	publisher := &publisherStub{}

	processor := NewProcessor(repository, publisher)

	found, err := processor.ProcessNext(context.Background())
	if err != nil {
		t.Fatalf("ProcessNext() unexpected error: %v", err)
	}

	if !found {
		t.Fatal("ProcessNext() processed = false, want true")
	}

	if !publisher.called {
		t.Fatal("publisher was not called")
	}

	if publisher.key != event.AggregateID {
		t.Errorf(
			"publisher key = %q, want %q",
			publisher.key,
			event.AggregateID,
		)
	}

	if !bytes.Equal(publisher.value, event.Payload) {
		t.Errorf(
			"publisher value = %s, want %s",
			publisher.value,
			event.Payload,
		)
	}
}

func TestProcessorProcessNextReturnsPublisherError(t *testing.T) {
	publisherErr := errors.New("Kafka unavailable")

	repository := &repositoryStub{
		event: Event{
			ID:          "event-1",
			AggregateID: "order-1",
		},
		exists: true,
	}

	publisher := &publisherStub{
		err: publisherErr,
	}

	processor := NewProcessor(repository, publisher)

	found, err := processor.ProcessNext(context.Background())

	if !found {
		t.Fatal("ProcessNext() processed = false, want true")
	}

	if !errors.Is(err, publisherErr) {
		t.Fatalf(
			"ProcessNext() error = %v, want wrapped %v",
			err,
			publisherErr,
		)
	}
}

func TestProcessorProcessNextReturnsFalseWhenEmpty(t *testing.T) {
	repository := &repositoryStub{}
	publisher := &publisherStub{}

	processor := NewProcessor(repository, publisher)

	found, err := processor.ProcessNext(context.Background())
	if err != nil {
		t.Fatalf("ProcessNext() unexpected error: %v", err)
	}

	if found {
		t.Fatal("ProcessNext() processed = true, want false")
	}

	if publisher.called {
		t.Fatal("publisher was called without an event")
	}
}
