package main

import (
	"context"
	"log"
	"net"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	orderv1 "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/api/order/v1"
	domainorder "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/order"
	postgresstorage "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/storage/postgres"
	grpctransport "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/transport/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	databaseConnectTimeout = 5 * time.Second
)

func main() {
	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort == "" {
		log.Fatal("GRPC_PORT is not set")
	}

	grpcAddress := ":" + grpcPort

	databaseUrl := os.Getenv("DATABASE_URL")
	if databaseUrl == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	connectCtx, cancel := context.WithTimeout(context.Background(), databaseConnectTimeout)
	defer cancel()

	pool, err := pgxpool.New(connectCtx, databaseUrl)
	if err != nil {
		log.Fatalf("create PostgreSQL connection pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(connectCtx); err != nil {
		log.Fatalf("ping PostgreSQL: %v", err)
	}

	listener, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		log.Fatalf("listen on %s: %v", grpcAddress, err)
	}

	repository := postgresstorage.NewOrderRepository(pool)

	orderService := domainorder.NewService(
		repository,
		uuid.NewString,
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
