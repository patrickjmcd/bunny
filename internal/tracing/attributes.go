package tracing

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

type Op string

const (
	OperationProduce Op = "produce"
	OperationConsume Op = "consume"
	OperationDelay   Op = "delay"

	MessagingSystemKeyValue = "rabbitmq"
)

func SystemKey() attribute.KeyValue {
	return semconv.MessagingSystemKey.String(MessagingSystemKeyValue)
}

func Operation(operationName Op) attribute.KeyValue {
	return semconv.MessagingOperationKey.String(string(operationName))
}

func DestinationTopic(topic string) attribute.KeyValue {
	return semconv.MessagingDestinationKey.String(topic)
}

func MessageKey(messageID string) attribute.KeyValue {
	return semconv.MessagingMessageIDKey.String(messageID)
}

func MessageHeaders(headers amqp.Table) []attribute.KeyValue {
	var attributes []attribute.KeyValue

	for k, v := range headers {
		switch t := v.(type) {
		case string:
			attributes = append(attributes, attribute.Key("messaging.headers."+k).String(t))
		case []byte:
			attributes = append(attributes, attribute.Key("messaging.headers."+k).String(string(t)))
		}
	}

	return attributes
}

func RoutingKey(routingKey string) attribute.KeyValue {
	return semconv.MessagingRabbitmqRoutingKeyKey.String(routingKey)
}
