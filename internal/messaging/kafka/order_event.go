package kafka

import (
	"encoding/json"
	"fmt"
)

func DecodeOrderCreated(value []byte) (OrderCreatedEvent, error) {
	var event OrderCreatedEvent

	if err := json.Unmarshal(value, &event); err != nil {
		return OrderCreatedEvent{}, fmt.Errorf("decode JSON: %w", err)
	}

	switch {
	case event.EventID == "":
		return OrderCreatedEvent{}, fmt.Errorf("event ID is required")

	case event.EventType != OrderCreatedEventType:
		return OrderCreatedEvent{}, fmt.Errorf("unexpected event type %v", event.EventType)

	case event.EventVersion != OrderCreatedEventVersion:
		return OrderCreatedEvent{}, fmt.Errorf("unexpected event version %v", event.EventVersion)

	case event.OrderID == "":
		return OrderCreatedEvent{}, fmt.Errorf("order ID is required")
	}

	return event, nil
}
