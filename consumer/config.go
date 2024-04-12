package consumer

import (
	"context"
	"time"

	"github.com/patrickjmcd/bunny/rabbit"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/patrickjmcd/bunny/internal/metrics"
	"github.com/patrickjmcd/bunny/rabbit/options"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type RabbitConsumerOption func(*RabbitConsumer)

func WithProtocol(protocol string) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.connectionOpts.Protocol = protocol
	}
}

func WithHost(host string) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.connectionOpts.Host = host
	}
}

func WithPort(port string) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.connectionOpts.Port = port
	}
}

func WithVHost(vhost string) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.connectionOpts.Port = vhost
	}
}

func WithUsername(username string) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.connectionOpts.Username = username
	}
}

func WithConnectionString(connectionString string) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.connectionOpts.ConnectionString = connectionString
	}
}

func WithTopic(topic string) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.Topic = topic
	}
}

func WithExchangeName(exchangeName string) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.exchangeOpts.Name = exchangeName
	}
}

func WithExchangeType(exchangeType string) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.exchangeOpts.Type = exchangeType
	}
}

func WithExchangeDurable(durable bool) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.exchangeOpts.Durable = durable
	}
}

func WithExchangeAutoDelete(autoDelete bool) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.exchangeOpts.AutoDelete = autoDelete
	}
}

func WithExchangeInternal(internal bool) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.exchangeOpts.Internal = internal
	}
}

func WithExchangeNoWait(noWait bool) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.exchangeOpts.NoWait = noWait
	}
}

func WithExchangeArgs(args amqp.Table) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.exchangeOpts.Args = args
	}
}

func WithQueueName(queueName string) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.queueOpts.Name = queueName
	}
}

func WithQueueDurable(durable bool) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.queueOpts.Durable = durable
	}
}

func WithQueueAutoDelete(autoDelete bool) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.queueOpts.AutoDelete = autoDelete
	}
}

func WithQueueExclusive(exclusive bool) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.queueOpts.Exclusive = exclusive
	}
}

func WithQueueNoWait(noWait bool) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.queueOpts.NoWait = noWait
	}
}

func WithQueueArgs(args amqp.Table) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.queueOpts.Args = args
	}
}

func WithQueueNoBind(noBind bool) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.queueOpts.NoBind = noBind
	}
}

func WithConsumerName(consumerName string) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.consumerOpts.Name = consumerName
	}
}

func WithConsumerAutoAck(autoAck bool) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.consumerOpts.AutoAck = autoAck
	}
}

func WithConsumerExclusive(exclusive bool) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.consumerOpts.Exclusive = exclusive
	}
}

func WithConsumerNoLocal(noLocal bool) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.consumerOpts.NoLocal = noLocal
	}
}

func WithConsumerNoWait(noWait bool) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.consumerOpts.NoWait = noWait
	}
}

func WithConsumerArgs(args amqp.Table) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.consumerOpts.Args = args
	}
}

func WithTracer(tracer oteltrace.Tracer) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.Tracer = tracer
	}
}

func WithTracePropagator(propagator propagation.TextMapPropagator) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.TracePropagator = propagator
	}
}

func WithValueDeserializer(valueDeserializer ValueDeserializer) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.ValueDeserializer = valueDeserializer
	}
}

func WithMessageHandler(handler MessageHandler) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.MessageHandler = handler
	}
}

func WithProcessingDelay(delay time.Duration) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.ProcessingDelay = delay
	}
}

func WithMessageErrorHandler(handler MessageErrorHandler) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.MessageErrorHandler = handler
	}
}

func WithPrefetchCount(prefetchCount int) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.consumerOpts.PrefetchCount = prefetchCount
	}
}

func WithSuppressProcessingErrors(suppress bool) RabbitConsumerOption {
	return func(rc *RabbitConsumer) {
		rc.supressProcessingErrors = suppress
	}
}

func NewRabbitConsumer(opts ...RabbitConsumerOption) (*RabbitConsumer, error) {
	rc := &RabbitConsumer{
		connectionOpts: options.ConnectionOptions{
			Protocol: "amqp",
			Username: "guest",
			Password: "guest",
			Host:     "localhost",
			Port:     "5672",
			Vhost:    "/",
		},
		Topic: "unconfigured-Topic",
		exchangeOpts: options.ExchangeOptions{
			Name:       "",
			Type:       "",
			Durable:    false,
			AutoDelete: false,
			Internal:   false,
			NoWait:     false,
			Args:       nil,
		},
		queueOpts: options.QueueOptions{
			Name:       "",
			Durable:    false,
			AutoDelete: true,
			Exclusive:  false,
			NoWait:     false,
			Args:       nil,
		},
		consumerOpts: options.ConsumerOptions{
			Name:          "",
			AutoAck:       false,
			Exclusive:     false,
			NoLocal:       false,
			NoWait:        false,
			Args:          nil,
			PrefetchCount: 0,
		},
	}

	noopTraceProvider := noop.NewTracerProvider()
	rc.Tracer = noopTraceProvider.Tracer("noop")
	rc.TracePropagator = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})

	for _, option := range opts {
		option(rc)
	}

	otel.SetTextMapPropagator(rc.TracePropagator)

	rc.Ready = make(chan bool, 1)

	rc.client = rabbit.New(
		rc.connectionOpts,
		rc.exchangeOpts,
		rc.queueOpts,
		rc.Topic,
		nil,
		&rc.consumerOpts,
		rc.Ready,
	)

	rc.Metrics = metrics.NewConsumerMetrics(rc.exchangeOpts.Name, rc.queueOpts.Name, rc.Topic)

	return rc, nil
}

type NoOpMessageHandler struct{}

func (n *NoOpMessageHandler) OnReceive(ctx context.Context, key, value interface{}) error {
	return nil
}

type NoOpMessageErrorHandler struct{}

func (n *NoOpMessageErrorHandler) OnError(ctx context.Context, raw *amqp.Delivery) error {
	log.Warn().Msg("noop error handler invoked")
	return nil
}
