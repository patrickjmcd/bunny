package consumer

import (
	"context"
	"github.com/patrickjmcd/bunny/internal/metrics"
	"github.com/patrickjmcd/bunny/rabbit"
	"github.com/patrickjmcd/bunny/rabbit/options"
	"go.opentelemetry.io/otel/propagation"
	"golang.org/x/sync/errgroup"
	"time"

	"github.com/patrickjmcd/bunny/internal/tracing"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type MessageHandler interface {
	OnReceive(ctx context.Context, key, value interface{}) error
}

type MessageErrorHandler interface {
	OnError(ctx context.Context, raw *amqp.Delivery) error
}

type RabbitConsumer struct {
	ValueDeserializer ValueDeserializer
	Tracer            oteltrace.Tracer
	TracePropagator   propagation.TextMapPropagator
	Metrics           *metrics.ConsumerMetrics
	Topic             string
	Ready             chan bool

	MessageHandler      MessageHandler
	MessageErrorHandler MessageErrorHandler
	ProcessingDelay     time.Duration

	client *rabbit.Client

	connectionOpts options.ConnectionOptions
	exchangeOpts   options.ExchangeOptions
	queueOpts      options.QueueOptions
	consumerOpts   options.ConsumerOptions
}

func (rc *RabbitConsumer) Run(ctx context.Context) error {
	allowedRetries := 3
	consumeRetries := 0
	deliveries, err := rc.client.Consume()
	if err != nil {
		log.Error().Err(err).Msg("Could not start consuming")
		return err
	}
	log.Debug().Msgf("consumer started on exchange: %s, queue: %s, topic %s", rc.exchangeOpts.Name, rc.queueOpts.Name, rc.Topic)

	// This channel will receive a notification when a channel closed event
	// happens. This must be different from Client.notifyChanClose because the
	// library sends only one notification and Client.notifyChanClose already has
	// a receiver in handleReconnect().
	// Recommended to make it buffered to avoid deadlocks
	chClosedCh := make(chan *amqp.Error, 1)
	rc.client.SetNotifyClose(chClosedCh)

	g, gctx := errgroup.WithContext(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-gctx.Done():
			return gctx.Err()

		case rabbitErr := <-chClosedCh:
			// This case handles the event of closed channel e.g. abnormal shutdown
			log.Error().Err(rabbitErr).Msg("channel closed")
			deliveries, err = rc.client.Consume()
			if err != nil {
				if consumeRetries > allowedRetries {
					log.Error().Msg("too many retries, giving up")
					return ErrConsumerRetryLimitReached
				}
				// If the AMQP channel is not ready, it will continue the loop. Next
				// iteration will enter this case because chClosedCh is closed by the
				// library
				log.Error().Err(err).Msgf("Could not start consuming, will try again (attempt %d of %d)", consumeRetries, allowedRetries)
				<-time.After(5 * time.Second)
				consumeRetries++
				continue
			}

		case d := <-deliveries:
			consumeRetries = 0
			rc.Metrics.ReceiveLatency.Observe(float64(time.Since(d.Timestamp).Milliseconds()))

			g.Go(func() error {
				if rc.ProcessingDelay > 0 {
					log.Debug().Msgf("processing delay of %s on message id: %s", rc.ProcessingDelay, d.MessageId)
					_, span := rc.startSpan(tracing.OperationDelay, &d)
					<-time.After(rc.ProcessingDelay)
					tracing.EndSpan(span, nil)
				}

				start := time.Now()
				ctx, span := rc.startSpanWithContext(gctx, tracing.OperationConsume, &d)
				invocationErr := rc.invokeMessageHandler(ctx, &d)
				if invocationErr != nil {
					err := rc.MessageErrorHandler.OnError(ctx, &d)
					if err != nil {
						log.Error().Err(err).Msg("message error handler failed processing")
						tracing.EndSpan(span, invocationErr)
						return ErrMessageErrorHandler
					}
				}
				rc.Metrics.ProcessingDuration.Observe(float64(time.Since(start).Milliseconds()))
				tracing.EndSpan(span, invocationErr)
				return nil
			})
		}
	}
}

func (rc *RabbitConsumer) Close() error {
	log.Info().Msg("closing consumer...")
	if err := rc.client.Close(); err != nil {
		log.Error().Err(err).Msg("failed to close rabbitmq consumer")
		return ErrConsumerClose
	}
	log.Info().Msg("consumer closed")
	return nil
}

func (rc *RabbitConsumer) invokeMessageHandler(ctx context.Context, msg *amqp.Delivery) error {

	value, err := rc.ValueDeserializer.Deserialize(ctx, msg.RoutingKey, msg.Body)
	if err != nil {
		log.Error().Err(err).Msg("failed to deserialize message")
		return ErrValueDeserialization
	}

	err = rc.MessageHandler.OnReceive(ctx, msg.CorrelationId, value)
	if err != nil {
		log.Error().Err(err).Msg("message handler failed processing")
		return ErrMessageHandler
	} else {
		if !rc.consumerOpts.AutoAck {
			err = msg.Ack(false)
			if err != nil {
				log.Error().Err(err).Msg("failed to acknowledge message")
				return ErrAck
			}
		}
	}

	return nil
}

func (rc *RabbitConsumer) startSpan(operationName tracing.Op, msg *amqp.Delivery) (context.Context, oteltrace.Span) {
	opts := []oteltrace.SpanStartOption{
		oteltrace.WithSpanKind(oteltrace.SpanKindConsumer),
	}

	carrier := tracing.NewMessageCarrier(msg)
	ctx := rc.TracePropagator.Extract(context.Background(), carrier)

	ctx, span := rc.Tracer.Start(ctx, string(operationName), opts...)

	rc.TracePropagator.Inject(ctx, carrier)

	span.SetAttributes(rc.attrsByOperationAndMessage(operationName, msg)...)

	return ctx, span
}

func (rc *RabbitConsumer) startSpanWithContext(ctx context.Context, operationName tracing.Op, msg *amqp.Delivery) (context.Context, oteltrace.Span) {
	opts := []oteltrace.SpanStartOption{
		oteltrace.WithSpanKind(oteltrace.SpanKindConsumer),
	}

	carrier := tracing.NewMessageCarrier(msg)
	ctx = rc.TracePropagator.Extract(ctx, carrier)

	ctx, span := rc.Tracer.Start(ctx, string(operationName), opts...)

	rc.TracePropagator.Inject(ctx, carrier)

	span.SetAttributes(rc.attrsByOperationAndMessage(operationName, msg)...)

	return ctx, span
}

func (rc *RabbitConsumer) attrsByOperationAndMessage(operation tracing.Op, msg *amqp.Delivery) []attribute.KeyValue {
	attributes := []attribute.KeyValue{
		tracing.SystemKey(),
		tracing.Operation(operation),
		semconv.MessagingDestinationKindTopic,
	}

	if msg != nil {
		attributes = append(attributes, tracing.RoutingKey(msg.RoutingKey))
		attributes = append(attributes, tracing.MessageHeaders(msg.Headers)...)
		attributes = append(attributes, tracing.DestinationTopic(rc.Topic))
	}

	return attributes
}
