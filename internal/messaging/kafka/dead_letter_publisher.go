package kafka

import (
	"context"
	"fmt"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	deadLetterOriginalTopicHeader     = "dlq-original-topic"
	deadLetterOriginalPartitionHeader = "dlq-original-partition"
	deadLetterOriginalOffsetHeader    = "dlq-original-offset"
	deadLetterErrorHeader             = "dlq-error"
)

type DeadLetterPublisher struct {
	producer SyncProducer
	topic    string
}

func NewDeadLetterPublisher(
	producer SyncProducer,
	topic string,
) *DeadLetterPublisher {
	return &DeadLetterPublisher{
		producer: producer,
		topic:    topic,
	}
}

func (p *DeadLetterPublisher) Publish(
	ctx context.Context,
	sourceRecord *kgo.Record,
	cause error,
) error {
	if sourceRecord == nil {
		return fmt.Errorf("source record is required")
	}

	if cause == nil {
		return fmt.Errorf("failure cause is required")
	}

	headers := make(
		[]kgo.RecordHeader,
		len(sourceRecord.Headers),
		len(sourceRecord.Headers)+4,
	)
	copy(headers, sourceRecord.Headers)

	headers = append(
		headers,
		kgo.RecordHeader{
			Key:   deadLetterOriginalTopicHeader,
			Value: []byte(sourceRecord.Topic),
		},
		kgo.RecordHeader{
			Key: deadLetterOriginalPartitionHeader,
			Value: []byte(strconv.FormatInt(
				int64(sourceRecord.Partition),
				10,
			)),
		},
		kgo.RecordHeader{
			Key: deadLetterOriginalOffsetHeader,
			Value: []byte(strconv.FormatInt(
				sourceRecord.Offset,
				10,
			)),
		},
		kgo.RecordHeader{
			Key:   deadLetterErrorHeader,
			Value: []byte(cause.Error()),
		},
	)

	record := &kgo.Record{
		Topic:   p.topic,
		Key:     sourceRecord.Key,
		Value:   sourceRecord.Value,
		Headers: headers,
	}

	if err := p.producer.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce dead-letter record: %w", err)
	}

	return nil
}
