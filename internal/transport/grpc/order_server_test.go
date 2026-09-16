package grpctransport

import (
	"context"
	"errors"
	"testing"
	"time"

	orderv1 "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/api/order/v1"
	domainorder "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/order"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type orderCreatorStub struct {
	createFunc func(
		ctx context.Context,
		input domainorder.CreateInput,
	) (domainorder.Order, error)
	getByIDFunc func(
		ctx context.Context,
		id string,
	) (domainorder.Order, error)
}

func (s *orderCreatorStub) Create(
	ctx context.Context,
	input domainorder.CreateInput,
) (domainorder.Order, error) {
	return s.createFunc(ctx, input)
}

func (s *orderCreatorStub) GetByID(
	ctx context.Context,
	id string,
) (domainorder.Order, error) {
	return s.getByIDFunc(ctx, id)
}

func TestOrderServerCreateOrder(t *testing.T) {
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

	creator := &orderCreatorStub{
		createFunc: func(
			_ context.Context,
			input domainorder.CreateInput,
		) (domainorder.Order, error) {
			if input.CustomerID != "customer-1" {
				t.Errorf(
					"CustomerID = %q, want %q",
					input.CustomerID,
					"customer-1",
				)
			}

			if input.AmountKopecks != 10_050 {
				t.Errorf(
					"AmountKopecks = %d, want %d",
					input.AmountKopecks,
					10_050,
				)
			}

			return domainorder.Order{
				ID:            "order-1",
				CustomerID:    input.CustomerID,
				AmountKopecks: input.AmountKopecks,
				Status:        domainorder.StatusPending,
				CreatedAt:     createdAt,
			}, nil
		},
	}

	server := NewOrderServer(creator)

	response, err := server.CreateOrder(
		context.Background(),
		&orderv1.CreateOrderRequest{
			CustomerId:    "customer-1",
			AmountKopecks: 10_050,
		},
	)
	if err != nil {
		t.Fatalf("CreateOrder() unexpected error: %v", err)
	}

	got := response.GetOrder()

	if got.GetId() != "order-1" {
		t.Errorf("ID = %q, want %q", got.GetId(), "order-1")
	}

	if got.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_PENDING {
		t.Errorf(
			"Status = %v, want %v",
			got.GetStatus(),
			orderv1.OrderStatus_ORDER_STATUS_PENDING,
		)
	}

	if !got.GetCreatedAt().AsTime().Equal(createdAt) {
		t.Errorf(
			"CreatedAt = %v, want %v",
			got.GetCreatedAt().AsTime(),
			createdAt,
		)
	}
}

func TestOrderServerCreateOrderMapsValidationError(t *testing.T) {
	creator := &orderCreatorStub{
		createFunc: func(
			_ context.Context,
			_ domainorder.CreateInput,
		) (domainorder.Order, error) {
			return domainorder.Order{}, domainorder.ErrCustomerIDRequired
		},
	}

	server := NewOrderServer(creator)

	_, err := server.CreateOrder(
		context.Background(),
		&orderv1.CreateOrderRequest{},
	)

	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf(
			"error code = %v, want %v",
			status.Code(err),
			codes.InvalidArgument,
		)
	}
}

func TestOrderServerCreateOrderHidesInternalError(t *testing.T) {
	creator := &orderCreatorStub{
		createFunc: func(
			_ context.Context,
			_ domainorder.CreateInput,
		) (domainorder.Order, error) {
			return domainorder.Order{}, errors.New("database unavailable")
		},
	}

	server := NewOrderServer(creator)

	_, err := server.CreateOrder(
		context.Background(),
		&orderv1.CreateOrderRequest{
			CustomerId:    "customer-1",
			AmountKopecks: 10_050,
		},
	)

	if status.Code(err) != codes.Internal {
		t.Fatalf(
			"error code = %v, want %v",
			status.Code(err),
			codes.Internal,
		)
	}

	if status.Convert(err).Message() != "internal server error" {
		t.Errorf(
			"error message = %q, want %q",
			status.Convert(err).Message(),
			"internal server error",
		)
	}
}

func TestOrderServerGetOrder(t *testing.T) {
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

	orderService := &orderCreatorStub{
		getByIDFunc: func(
			_ context.Context,
			id string,
		) (domainorder.Order, error) {
			return domainorder.Order{
				ID:            id,
				CustomerID:    "customer-1",
				AmountKopecks: 10_050,
				Status:        domainorder.StatusPending,
				CreatedAt:     createdAt,
			}, nil
		},
	}

	server := NewOrderServer(orderService)

	response, err := server.GetOrder(
		context.Background(),
		&orderv1.GetOrderRequest{
			Id: "00000000-0000-4000-8000-000000000001",
		},
	)
	if err != nil {
		t.Fatalf("GetOrder() unexpected error: %v", err)
	}

	got := response.GetOrder()

	if got.GetId() != "00000000-0000-4000-8000-000000000001" {
		t.Errorf("ID = %q", got.GetId())
	}

	if got.GetCustomerId() != "customer-1" {
		t.Errorf(
			"CustomerID = %q, want %q",
			got.GetCustomerId(),
			"customer-1",
		)
	}

	if got.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_PENDING {
		t.Errorf(
			"Status = %v, want %v",
			got.GetStatus(),
			orderv1.OrderStatus_ORDER_STATUS_PENDING,
		)
	}
}

func TestOrderServerGetOrderMapsNotFound(t *testing.T) {
	orderService := &orderCreatorStub{
		getByIDFunc: func(
			_ context.Context,
			_ string,
		) (domainorder.Order, error) {
			return domainorder.Order{},
				domainorder.ErrOrderNotFound
		},
	}

	server := NewOrderServer(orderService)

	_, err := server.GetOrder(
		context.Background(),
		&orderv1.GetOrderRequest{
			Id: "00000000-0000-4000-8000-000000000099",
		},
	)

	if status.Code(err) != codes.NotFound {
		t.Fatalf(
			"error code = %v, want %v",
			status.Code(err),
			codes.NotFound,
		)
	}
}
