package producer

import (
	"context"
	"github.com/patrickjmcd/bunny/internal/metrics"
	"github.com/patrickjmcd/bunny/internal/tracing"
	"github.com/patrickjmcd/bunny/rabbit"
	"github.com/patrickjmcd/bunny/rabbit/options"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type RabbitProducer struct {
	ValueSerializer ValueSerializer
	Tracer          oteltrace.Tracer
	TracePropagator propagation.TextMapPropagator
	Metrics         *metrics.ProducerMetrics
	Topic           string
	Ready           chan bool

	client *rabbit.Client

	connectionOpts options.ConnectionOptions
	exchangeOpts   options.ExchangeOptions
	queueOpts      options.QueueOptions
	publisherOpts  options.PublisherOptions
}

func (rp *RabbitProducer) ProduceMessage(ctx context.Context, key, value interface{}, replyTo string) error {
	valuePayload, err := rp.ValueSerializer.Serialize(rp.Topic, value)
	if err != nil {
		log.Error().Err(err).Interface("value", value).Msg("failed to serialize value")
		return ErrSerialization
	}

	publishMsg := amqp.Publishing{
		ContentType:   rp.ValueSerializer.GetContentType(),
		Body:          valuePayload,
		DeliveryMode:  amqp.Persistent,
		CorrelationId: key.(string),
	}

	if replyTo != "" {
		publishMsg.ReplyTo = replyTo
	}

	return rp.ProduceRaw(ctx, key, &publishMsg)
}

func (rp *RabbitProducer) ProduceRaw(ctx context.Context, key interface{}, value *amqp.Publishing) error {
	s := rp.startSpan(ctx, tracing.OperationProduce, value)
	if err := rp.client.Push(*value); err != nil {
		tracing.EndSpan(s, err)
		log.Error().Err(err).Msg("failed to produce message")
		return ErrProducerError
	}
	tracing.EndSpan(s, nil)
	rp.Metrics.Total.Inc()
	return nil
}

// Close closes the producer and waits for the last outstanding produce requests to finish
func (rp *RabbitProducer) Close() {
	log.Info().Msg("closing producer...")

	err := rp.client.Close()
	if err != nil {
		log.Error().Err(err).Msg("failed to close rabbitmq producer")
	} else {
		log.Info().Msg("producer closed")
	}
}

// startSpan starts a new span for the given operation
func (rp *RabbitProducer) startSpan(ctx context.Context, operationName tracing.Op, msg *amqp.Publishing) oteltrace.Span {
	opts := []oteltrace.SpanStartOption{
		oteltrace.WithSpanKind(oteltrace.SpanKindProducer),
	}

	carrier := tracing.NewProducerMessageCarrier(msg)
	ctx = rp.TracePropagator.Extract(ctx, carrier)

	ctx, span := rp.Tracer.Start(ctx, string(operationName), opts...)

	rp.TracePropagator.Inject(ctx, carrier)

	span.SetAttributes(rp.attrsByOperationAndMessage(operationName, msg)...)

	return span
}

// attrsByOperationAndMessage returns a slice of attributes for the given operation and message
func (rp *RabbitProducer) attrsByOperationAndMessage(operation tracing.Op, msg *amqp.Publishing) []attribute.KeyValue {
	attributes := []attribute.KeyValue{
		tracing.SystemKey(),
		tracing.Operation(operation),
		semconv.MessagingDestinationKindTopic,
	}

	if msg != nil {
		attributes = append(attributes, tracing.MessageHeaders(msg.Headers)...)
		attributes = append(attributes, tracing.DestinationTopic(rp.Topic))
	}

	return attributes
}
