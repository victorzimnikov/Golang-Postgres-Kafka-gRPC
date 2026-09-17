package outbox

import (
	"context"
	"fmt"
	"time"
)

type Event struct {
	ID          string
	AggregateID string
	Payload     []byte
	OccurredAt  time.Time
}

type Handler func(
	ctx context.Context,
	event Event,
) error

type Repository interface {
	ProcessNext(
		ctx context.Context,
		handler Handler,
	) (bool, error)
}

type Publisher interface {
	Publish(
		ctx context.Context,
		Key string,
		value []byte,
		occurredAt time.Time,
	) error
}

type Processor struct {
	repository Repository
	publisher  Publisher
}

func NewProcessor(
	repository Repository,
	publisher Publisher,
) *Processor {
	return &Processor{
		repository: repository,
		publisher:  publisher,
	}
}

func (p *Processor) ProcessNext(ctx context.Context) (bool, error) {
	found, err := p.repository.ProcessNext(
		ctx,
		func(ctx context.Context, event Event) error {
			err := p.publisher.Publish(
				ctx,
				event.AggregateID,
				event.Payload,
				event.OccurredAt,
			)
			if err != nil {
				return fmt.Errorf(
					"publish event %s: %w",
					event.ID,
					err,
				)
			}

			return nil
		},
	)
	if err != nil {
		return found, fmt.Errorf(
			"process next outbox event: %w",
			err,
		)
	}

	return found, nil
}
