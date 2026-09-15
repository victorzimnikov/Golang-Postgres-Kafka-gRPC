package order

import (
	"errors"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	createdAt := time.Date(2026, time.September, 15, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		id            string
		customerID    string
		amountKopecks int64
		wantErr       error
	}{
		{
			name:          "creates valid order",
			id:            "order-1",
			customerID:    "customer-1",
			amountKopecks: 10_050,
		},
		{
			name:          "rejects empty order ID",
			customerID:    "customer-1",
			amountKopecks: 10_050,
			wantErr:       ErrIDRequired,
		},
		{
			name:          "rejects empty customer ID",
			id:            "order-1",
			amountKopecks: 10_050,
			wantErr:       ErrCustomerIDRequired,
		},
		{
			name:       "rejects zero amount",
			id:         "order-1",
			customerID: "customer-1",
			wantErr:    ErrAmountNotPositive,
		},
		{
			name:          "rejects negative amount",
			id:            "order-1",
			customerID:    "customer-1",
			amountKopecks: -100,
			wantErr:       ErrAmountNotPositive,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.id, tt.customerID, tt.amountKopecks, createdAt)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("New() error = %v, want %v", err, tt.wantErr)
			}

			if tt.wantErr != nil {
				return
			}

			if got.ID != tt.id {
				t.Errorf("ID = %q, want %q", got.ID, tt.id)
			}

			if got.CustomerID != tt.customerID {
				t.Errorf("CustomerID = %q, want %q", got.CustomerID, tt.customerID)
			}

			if got.AmountKopecks != tt.amountKopecks {
				t.Errorf("AmountKopecks = %d, want %d", got.AmountKopecks, tt.amountKopecks)
			}

			if got.Status != StatusPending {
				t.Errorf("Status = %q, want %q", got.Status, StatusPending)
			}

			if !got.CreatedAt.Equal(createdAt) {
				t.Errorf("CreatedAt = %v, want %v", got.CreatedAt, createdAt)
			}
		})
	}
}
