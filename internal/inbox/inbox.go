package inbox

import "context"

type Message struct {
	ConsumerGroup string
	EventID       string
	Topic         string
	Partition     int32
	Offset        int64
}

type Repository interface {
	TryMarkProcessed(
		ctx context.Context,
		message Message,
	) (bool, error)
}
