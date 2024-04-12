package retry

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestMessagePublisher struct{}

func (nop *TestMessagePublisher) ProduceMessage(ctx context.Context, key, value interface{}) error {
	return nil
}
func (nop *TestMessagePublisher) ProduceRaw(ctx context.Context, key interface{}, msg *amqp.Publishing) error {
	return nil
}
func (nop *TestMessagePublisher) Close() {}

func TestWithMessagePublisher(t *testing.T) {
	publisher := &TestMessagePublisher{}
	rp := &RetryPublisher{}

	WithMessagePublisher(publisher)(rp)

	assert.Equal(t, publisher, rp.MessagePublisher)
}

func Test_NewRetryPublisherConfig_WithMessagePublisher(t *testing.T) {
	publisher := &TestMessagePublisher{}
	rp := NewRetryPublisherConfig(WithMessagePublisher(publisher))
	assert.Equal(t, publisher, rp.MessagePublisher)
}

func Test_NewRetryPublisherConfig_DefaultPublisher(t *testing.T) {
	publisher := &NoOpMessagePublisher{}
	rp := NewRetryPublisherConfig()
	assert.Equal(t, publisher, rp.MessagePublisher)
}
