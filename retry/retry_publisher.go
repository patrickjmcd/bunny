package retry

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

type MessagePublisher interface {
	ProduceRaw(ctx context.Context, key interface{}, msg *amqp.Publishing) error
	Close()
}

type RetryPublisher struct {
	MessagePublisher MessagePublisher
}

func (r *RetryPublisher) OnError(ctx context.Context, raw *amqp.Delivery) error {

	msg := &amqp.Publishing{
		Body:          raw.Body,
		ContentType:   raw.ContentType,
		Headers:       raw.Headers,
		CorrelationId: raw.CorrelationId,
		DeliveryMode:  raw.DeliveryMode,
	}

	err := r.MessagePublisher.ProduceRaw(ctx, nil, msg)
	if err != nil {
		log.Error().Err(err).Msg("failed to produce retry message")
		return ErrFailedToProduceRetryMessage
	}
	err = raw.Ack(false)
	if err != nil {
		log.Error().Err(err).Msg("failed to ack message")
		return ErrFailedToAckMessage
	}
	return nil
}
