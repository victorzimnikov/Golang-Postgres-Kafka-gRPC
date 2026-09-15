package grpctransport

import (
	"context"
	"errors"

	orderv1 "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/api/order/v1"
	domainorder "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/order"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type OrderCreator interface {
	Create(ctx context.Context, input domainorder.CreateInput) (domainorder.Order, error)
}

type OrderServer struct {
	orderv1.UnimplementedOrderServiceServer

	orderCreator OrderCreator
}

func NewOrderServer(orderCreator OrderCreator) *OrderServer {
	return &OrderServer{
		orderCreator: orderCreator,
	}
}

func (s *OrderServer) CreateOrder(ctx context.Context, request *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request os required")
	}

	createdOrder, err := s.orderCreator.Create(
		ctx,
		domainorder.CreateInput{
			CustomerID:    request.GetCustomerId(),
			AmountKopecks: request.GetAmountKopecks(),
		},
	)

	if err != nil {
		return nil, mapOrderError(err)
	}

	return &orderv1.CreateOrderResponse{
		Order: &orderv1.Order{
			Id:            createdOrder.ID,
			CustomerId:    createdOrder.CustomerID,
			AmountKopecks: createdOrder.AmountKopecks,
			Status:        mapOrderStatus(createdOrder.Status),
			CreatedAt:     timestamppb.New(createdOrder.CreatedAt),
		},
	}, nil
}

func mapOrderStatus(value domainorder.Status) orderv1.OrderStatus {
	switch value {
	case domainorder.StatusPending:
		return orderv1.OrderStatus_ORDER_STATUS_PENDING

	default:
		return orderv1.OrderStatus_ORDER_STATUS_UNSPECIFIED
	}
}

func mapOrderError(err error) error {
	switch {
	case errors.Is(err, domainorder.ErrIDRequired),
		errors.Is(err, domainorder.ErrCustomerIDRequired),
		errors.Is(err, domainorder.ErrAmountNotPositive):
		return status.Error(codes.InvalidArgument, err.Error())

	default:
		return status.Error(codes.Internal, "internal server error")
	}
}
