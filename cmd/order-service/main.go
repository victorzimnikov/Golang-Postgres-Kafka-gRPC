package main

import (
	"fmt"
	"log"
	"net"
	"sync/atomic"
	"time"

	orderv1 "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/api/order/v1"
	domainorder "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/order"
	"github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/storage/memory"
	grpctransport "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/transport/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const grpcAddress = ":50051"

func main() {
	listener, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		log.Fatalf("listen on %s: %v", grpcAddress, err)
	}

	repository := memory.NewOrderRepository()

	var orderCounter atomic.Uint64

	orderService := domainorder.NewService(
		repository,
		func() string {
			id := orderCounter.Add(1)

			return fmt.Sprintf("order-%d", id)
		},
		time.Now,
	)

	orderServer := grpctransport.NewOrderServer(orderService)

	grpcServer := grpc.NewServer()

	orderv1.RegisterOrderServiceServer(grpcServer, orderServer)
	reflection.Register(grpcServer)

	log.Printf("gRPC server listening on %s", grpcAddress)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("serve gRPC: %v", err)
	}
}
