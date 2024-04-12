package tracing

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/propagation"
)

var _ propagation.TextMapCarrier = (*MessageCarrier)(nil)

// MessageCarrier injects and extracts traces from an amqp.Delivery.
type MessageCarrier struct {
	msg *amqp.Delivery
}

// NewMessageCarrier creates a new MessageCarrier.
func NewMessageCarrier(msg *amqp.Delivery) MessageCarrier {
	return MessageCarrier{msg: msg}
}

// Get retrieves a single value for a given key from rabbit message headers.
func (c MessageCarrier) Get(key string) string {
	if v, ok := c.msg.Headers[key]; ok {
		switch t := v.(type) {
		case string:
			return t
		case []byte:
			return string(t)
		}
	}
	return ""
}

// Set sets a header on rabbit message.
func (c MessageCarrier) Set(key, value string) {
	if key == "" || value == "" {
		return
	}
	if c.msg.Headers == nil {
		c.msg.Headers = amqp.Table{}
	}

	c.msg.Headers[key] = value
}

// Keys returns all keys identifiers from the message headers.
func (c MessageCarrier) Keys() []string {
	var out []string
	for i := range c.msg.Headers {
		out = append(out, i)
	}
	return out
}
