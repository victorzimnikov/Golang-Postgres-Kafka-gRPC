package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/inbox"
	kafkamessaging "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/messaging/kafka"
	postgresstorage "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/storage/postgres"
)

const startupTimeout = 10 * time.Second

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return fmt.Errorf("DATABASE_URL is not set")
	}

	brokersValue := os.Getenv("KAFKA_BROKERS")
	if brokersValue == "" {
		return fmt.Errorf("KAFKA_BROKERS is not set")
	}

	topic := os.Getenv("KAFKA_ORDER_CREATED_TOPIC")
	if topic == "" {
		return fmt.Errorf("KAFKA_ORDER_CREATED_TOPIC is not set")
	}

	group := os.Getenv("KAFKA_CONSUMER_GROUP")
	if group == "" {
		return fmt.Errorf("KAFKA_CONSUMER_GROUP is not set")
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	startupCtx, cancelStartup := context.WithTimeout(ctx, startupTimeout)
	defer cancelStartup()

	pool, err := pgxpool.New(startupCtx, databaseURL)
	if err != nil {
		return fmt.Errorf("create PostgreSQL connection pool: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(startupCtx); err != nil {
		return fmt.Errorf("ping PostgreSQL: %w", err)
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokersValue, ",")...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return fmt.Errorf("create Kafka consumer: %w", err)
	}
	defer client.CloseAllowingRebalance()

	err = client.Ping(startupCtx)
	if err != nil {
		return fmt.Errorf("ping Kafka: %w", err)
	}

	cancelStartup()

	processedEvents := postgresstorage.NewProcessedEventRepository(pool)

	log.Printf(
		"consumer started: topic=%s group=%s",
		topic,
		group,
	)

	for {
		fetches := client.PollRecords(ctx, 1)

		if ctx.Err() != nil {
			return nil
		}

		if fetchErrors := fetches.Errors(); len(fetchErrors) > 0 {
			for _, fetchErr := range fetchErrors {
				log.Printf(
					"fetch error: topic=%s partition=%d: %v",
					fetchErr.Topic,
					fetchErr.Partition,
					fetchErr.Err,
				)
			}

			client.AllowRebalance()
			continue
		}

		for _, record := range fetches.Records() {
			if _, err := handleRecord(ctx, group, processedEvents, record); err != nil {
				client.AllowRebalance()

				return fmt.Errorf(
					"handle topic=%s partition=%d offset=%d: %w",
					record.Topic,
					record.Partition,
					record.Offset,
					err,
				)
			}

			if err := client.CommitRecords(ctx, record); err != nil {
				client.AllowRebalance()

				if ctx.Err() != nil {
					return nil
				}

				return fmt.Errorf(
					"commit topic=%s partition=%d offset=%d: %w",
					record.Topic,
					record.Partition,
					record.Offset,
					err,
				)
			}
		}

		client.AllowRebalance()
	}
}

func handleRecord(
	ctx context.Context,
	consumerGroup string,
	processedEvents inbox.Repository,
	record *kgo.Record,
) (bool, error) {
	event, err := kafkamessaging.DecodeOrderCreated(record.Value)
	if err != nil {
		return false, err
	}

	if string(record.Key) != event.OrderID {
		return false, fmt.Errorf(
			"record key %q does not mach order ID %q",
			record.Key,
			event.OrderID,
		)
	}

	firstProcessing, err := processedEvents.TryMarkProcessed(
		ctx,
		inbox.Message{
			ConsumerGroup: consumerGroup,
			EventID:       event.EventID,
			Topic:         record.Topic,
			Partition:     record.Partition,
			Offset:        record.Offset,
		},
	)
	if err != nil {
		return false, fmt.Errorf(
			"mark event %s processed: %w",
			event.EventID,
			err,
		)
	}

	if !firstProcessing {
		log.Printf(
			"duplicate event skipped: event_id=%s topic=%s partition=%d offset=%d",
			event.EventID,
			record.Topic,
			record.Partition,
			record.Offset,
		)

		return false, nil
	}

	log.Printf(
		"order created: order_id=%s customer_id=%s amount_kopecks=%d partition=%d offset=%d",
		event.OrderID,
		event.CustomerID,
		event.AmountKopecks,
		record.Partition,
		record.Offset,
	)

	return true, nil
}
