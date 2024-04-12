package retry

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RetryPublisherConfigOption func(*RetryPublisher)

func WithMessagePublisher(publisher MessagePublisher) RetryPublisherConfigOption {
	return func(rp *RetryPublisher) {
		rp.MessagePublisher = publisher
	}
}

func NewRetryPublisherConfig(options ...RetryPublisherConfigOption) *RetryPublisher {
	rp := &RetryPublisher{
		MessagePublisher: &NoOpMessagePublisher{},
	}

	for _, option := range options {
		option(rp)
	}

	return rp
}

type NoOpMessagePublisher struct{}

func (nop *NoOpMessagePublisher) ProduceRaw(ctx context.Context, key interface{}, msg *amqp.Publishing) error {
	return nil
}

func (nop *NoOpMessagePublisher) Close() {}
