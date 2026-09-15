package kafka

import "testing"

func TestDecodeOrderCreated(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{
			name: "decodes valid event",
			value: `{
				"event_id": "event-1",
				"event_type": "order.created",
				"event_version": 1,
				"occurred_at": "2026-09-15T12:00:00Z",
				"order_id": "order-1",
				"customer_id": "customer-1",
				"amount_kopecks": 10050,
				"status": "pending"
			}`,
		},
		{
			name:    "rejects invalid JSON",
			value:   `{`,
			wantErr: true,
		},
		{
			name: "rejects unsupported version",
			value: `{
				"event_id": "event-1",
				"event_type": "order.created",
				"event_version": 2,
				"order_id": "order-1"
			}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := DecodeOrderCreated(
				[]byte(tt.value),
			)

			if tt.wantErr {
				if err == nil {
					t.Fatal(
						"DecodeOrderCreated() expected error",
					)
				}

				return
			}

			if err != nil {
				t.Fatalf(
					"DecodeOrderCreated() unexpected error: %v",
					err,
				)
			}

			if event.OrderID != "order-1" {
				t.Errorf(
					"OrderID = %q, want %q",
					event.OrderID,
					"order-1",
				)
			}
		})
	}
}
