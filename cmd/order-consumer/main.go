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

	"github.com/twmb/franz-go/pkg/kgo"
	kafkamessaging "github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/messaging/kafka"
)

const kafkaConnectTimeout = 10 * time.Second

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
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

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokersValue, ",")...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return fmt.Errorf("create Kafka consumer: %w", err)
	}
	defer client.CloseAllowingRebalance()

	pingCtx, cancelPing := context.WithTimeout(
		ctx,
		kafkaConnectTimeout,
	)
	err = client.Ping(pingCtx)
	cancelPing()

	if err != nil {
		return fmt.Errorf("ping Kafka: %w", err)
	}

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
			if err := handleRecord(record); err != nil {
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

func handleRecord(record *kgo.Record) error {
	event, err := kafkamessaging.DecodeOrderCreated(record.Value)
	if err != nil {
		return err
	}

	if string(record.Key) != event.OrderID {
		return fmt.Errorf(
			"record key %q does not mach order ID %q",
			record.Key,
			event.OrderID,
		)
	}

	log.Printf(
		"order created: order_id=%s customer_id=%s amount_kopecks=%d partition=%d offset=%d",
		event.OrderID,
		event.CustomerID,
		event.AmountKopecks,
		record.Partition,
		record.Offset,
	)

	return nil
}
