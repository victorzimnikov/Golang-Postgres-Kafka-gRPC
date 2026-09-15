package main

import (
	"context"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	orderv1 "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/api/order/v1"
	kafkamessaging "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/messaging/kafka"
	domainorder "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/order"
	postgresstorage "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/storage/postgres"
	grpctransport "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/transport/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	grpcAddress            = ":50051"
	databaseConnectTimeout = 5 * time.Second
)

func main() {
	databaseUrl := os.Getenv("DATABASE_URL")
	if databaseUrl == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		log.Fatal("KAFKA_BROKERS is not set")
	}

	kafkaTopic := os.Getenv("KAFKA_ORDER_CREATED_TOPIC")
	if kafkaTopic == "" {
		log.Fatal("KAFKA_ORDER_CREATED_TOPIC is not set")
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

	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(kafkaBrokers, ",")...),
		kgo.RecordDeliveryTimeout(10*time.Second),
	)
	if err != nil {
		log.Fatalf("create Kafka client: %v", err)
	}
	defer kafkaClient.Close()

	if err := kafkaClient.Ping(connectCtx); err != nil {
		log.Fatalf("ping Kafka: %v", err)
	}

	listener, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		log.Fatalf("listen on %s: %v", grpcAddress, err)
	}

	repository := postgresstorage.NewOrderRepository(pool)

	publisher := kafkamessaging.NewOrderPublisher(
		kafkaClient,
		kafkaTopic,
	)

	orderService := domainorder.NewService(
		repository,
		publisher,
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
