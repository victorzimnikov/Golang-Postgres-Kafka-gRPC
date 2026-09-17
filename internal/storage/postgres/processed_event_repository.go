package postgres

import (
	"context"
	"fmt"

	"github.com/victorzimnikov/Golang-Postgres-Kafka-gRPC/internal/inbox"
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
	const query = `
		INSERT INTO processed_events (
			consumer_group,
			event_id,
			topic,
			partition,
			offset_value
		)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (consumer_group, event_id)
			DO NOTHING
	`

	tag, err := r.db.Exec(
		ctx,
		query,
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
