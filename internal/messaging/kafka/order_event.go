package kafka

import (
	"encoding/json"
	"fmt"

	domainorder "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/order"
)

func DecodeOrderCreated(value []byte) (domainorder.OrderCreatedEvent, error) {
	var event domainorder.OrderCreatedEvent

	if err := json.Unmarshal(value, &event); err != nil {
		return domainorder.OrderCreatedEvent{}, fmt.Errorf("decode JSON: %w", err)
	}

	switch {
	case event.EventID == "":
		return domainorder.OrderCreatedEvent{}, fmt.Errorf("event ID is required")

	case event.EventType != domainorder.OrderCreatedEventType:
		return domainorder.OrderCreatedEvent{}, fmt.Errorf("unexpected event type %v", event.EventType)

	case event.EventVersion != domainorder.OrderCreatedEventVersion:
		return domainorder.OrderCreatedEvent{}, fmt.Errorf("unexpected event version %v", event.EventVersion)

	case event.OrderID == "":
		return domainorder.OrderCreatedEvent{}, fmt.Errorf("order ID is required")
	}

	return event, nil
}
