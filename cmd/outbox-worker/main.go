package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	appconfig "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/config"
	kafkamessaging "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/messaging/kafka"
	"github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/outbox"
	postgresstorage "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/storage/postgres"
)

const (
	connectTimeout        = 10 * time.Second
	recordDeliveryTimeout = 10 * time.Second
	idlePollingInterval   = 500 * time.Millisecond
	errorRetryDelay       = 2 * time.Second
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	config, err := appconfig.LoadOutboxWorkerConfig()
	if err != nil {
		log.Fatal(err)
	}

	startupCtx, cancelStartup := context.WithTimeout(ctx, connectTimeout)

	pool, err := pgxpool.New(startupCtx, config.DatabaseURL)
	if err != nil {
		cancelStartup()
		log.Fatalf("create PostgreSQL connection pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(startupCtx); err != nil {
		log.Fatalf("ping PostgreSQL: %v", err)
	}

	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(config.KafkaBrokers, ",")...),
		kgo.RecordDeliveryTimeout(recordDeliveryTimeout),
	)
	if err != nil {
		cancelStartup()
		log.Fatalf("create Kafka client: %v", err)
	}
	defer kafkaClient.Close()

	if err := kafkaClient.Ping(startupCtx); err != nil {
		cancelStartup()
		log.Fatalf("ping Kafka: %v", err)
	}

	cancelStartup()

	repository := postgresstorage.NewOutboxRepository(pool)

	publisher := kafkamessaging.NewPublisher(
		kafkaClient,
		config.KafkaOrderCreatedTopic,
	)

	processor := outbox.NewProcessor(
		repository,
		publisher,
	)

	log.Printf(
		"outbox worker started for topic %v",
		config.KafkaOrderCreatedTopic,
	)

	for {
		found, err := processor.ProcessNext(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}

			log.Printf("process outbox event: %v", err)

			if !wait(ctx, errorRetryDelay) {
				break
			}

			continue
		}

		if found {
			continue
		}

		if !wait(ctx, idlePollingInterval) {
			break
		}
	}

	log.Print("outbox worker stopped")
}

func wait(
	ctx context.Context,
	duration time.Duration,
) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false

	case <-timer.C:
		return true
	}
}
