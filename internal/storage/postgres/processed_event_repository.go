package postgres

import (
	"context"
	"fmt"

	"github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/inbox"
	"github.com/victorzimnikov/pgqb"
)

type ProcessEventRepository struct {
	db DBTX
}

func NewProcessedEventRepository(db DBTX) *ProcessEventRepository {
	return &ProcessEventRepository{
		db: db,
	}
}

func (r *ProcessEventRepository) TryMarkProcessed(
	ctx context.Context,
	message inbox.Message,
) (bool, error) {
	tag, err := pgqb.
		NewBuilder(ctx, r.db).
		Insert(
			"processed_events",
			"consumer_group",
			"event_id",
			"topic",
			"partition",
			"offset_value",
		).
		ConflictNothing("consumer_group", "event_id").
		Exec(
			message.ConsumerGroup,
			message.EventID,
			message.Topic,
			message.Partition,
			message.Offset,
		)
	if err != nil {
		return false, fmt.Errorf("insert processed event: %w", err)
	}

	return tag.RowsAffected() == 1, nil
}

var _ inbox.Repository = (*ProcessEventRepository)(nil)
