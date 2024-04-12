package producer

import (
	"github.com/patrickjmcd/bunny/internal/metrics"
	"github.com/patrickjmcd/bunny/rabbit"
	"github.com/patrickjmcd/bunny/rabbit/options"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type RabbitProducerOption func(*RabbitProducer)

func WithProtocol(protocol string) RabbitProducerOption {
	return func(rp *RabbitProducer) {
		rp.connectionOpts.Protocol = protocol
	}
}

func WithHost(host string) RabbitProducerOption {
	return func(rp *RabbitProducer) {
		rp.connectionOpts.Host = host
	}
}

func WithPort(port string) RabbitProducerOption {
	return func(rp *RabbitProducer) {
		rp.connectionOpts.Port = port
	}
}

func WithVHost(vhost string) RabbitProducerOption {
	return func(rp *RabbitProducer) {
		rp.connectionOpts.Port = vhost
	}
}

func WithUsername(username string) RabbitProducerOption {
	return func(rp *RabbitProducer) {
		rp.connectionOpts.Username = username
	}
}

func WithConnectionString(connectionString string) RabbitProducerOption {
	return func(rp *RabbitProducer) {
		rp.connectionOpts.ConnectionString = connectionString
	}
}

func WithTopic(topic string) RabbitProducerOption {
	return func(rp *RabbitProducer) {
		rp.Topic = topic
	}
}

func WithExchangeName(exchangeName string) RabbitProducerOption {
	return func(rp *RabbitProducer) {
		rp.exchangeOpts.Name = exchangeName
	}
}

func WithExchangeType(exchangeType string) RabbitProducerOption {
	return func(rp *RabbitProducer) {
		rp.exchangeOpts.Type = exchangeType
	}
}

func WithExchangeDurable(durable bool) RabbitProducerOption {
	return func(rp *RabbitProducer) {
		rp.exchangeOpts.Durable = durable
	}
}

func WithExchangeAutoDelete(autoDelete bool) RabbitProducerOption {
	return func(rp *RabbitProducer) {
		rp.exchangeOpts.AutoDelete = autoDelete
	}
}

func WithExchangeInternal(internal bool) RabbitProducerOption {
	return func(rp *RabbitProducer) {
		rp.exchangeOpts.Internal = internal
	}
}

func WithExchangeNoWait(noWait bool) RabbitProducerOption {
	return func(rp *RabbitProducer) {
		rp.exchangeOpts.NoWait = noWait
	}
}

func WithExchangeArgs(args amqp.Table) RabbitProducerOption {
	return func(rp *RabbitProducer) {
		rp.exchangeOpts.Args = args
	}
}

func WithQueueName(queueName string) RabbitProducerOption {
	return func(rp *RabbitProducer) {
		rp.queueOpts.Name = queueName
	}
}

func WithQueueDurable(durable bool) RabbitProducerOption {
	return func(rp *RabbitProducer) {
		rp.queueOpts.Durable = durable
	}
}

func WithQueueAutoDelete(autoDelete bool) RabbitProducerOption {
	return func(rp *RabbitProducer) {
		rp.queueOpts.AutoDelete = autoDelete
	}
}

func WithQueueExclusive(exclusive bool) RabbitProducerOption {
	return func(rp *RabbitProducer) {
		rp.queueOpts.Exclusive = exclusive
	}
}

func WithQueueNoWait(noWait bool) RabbitProducerOption {
	return func(rp *RabbitProducer) {
		rp.queueOpts.NoWait = noWait
	}
}

func WithQueueArgs(args amqp.Table) RabbitProducerOption {
	return func(rp *RabbitProducer) {
		rp.queueOpts.Args = args
	}
}

func WithQueueNoBind(noBind bool) RabbitProducerOption {
	return func(rp *RabbitProducer) {
		rp.queueOpts.NoBind = noBind
	}
}

func WithPublisherMandatory(mandatory bool) RabbitProducerOption {
	return func(rp *RabbitProducer) {
		rp.publisherOpts.Mandatory = mandatory
	}
}

func WithPublisherImmediate(immediate bool) RabbitProducerOption {
	return func(rp *RabbitProducer) {
		rp.publisherOpts.Immediate = immediate
	}
}

func WithValueSerializer(valueSerializer ValueSerializer) RabbitProducerOption {
	return func(rp *RabbitProducer) {
		rp.ValueSerializer = valueSerializer
	}
}

func WithTracer(tracer oteltrace.Tracer) RabbitProducerOption {
	return func(srp *RabbitProducer) {
		srp.Tracer = tracer
	}
}

func WithTracePropagator(propagator propagation.TextMapPropagator) RabbitProducerOption {
	return func(srp *RabbitProducer) {
		srp.TracePropagator = propagator
	}
}

func NewRabbitProducer(opts ...RabbitProducerOption) (*RabbitProducer, error) {
	rp := &RabbitProducer{
		Topic: "unconfigured-topic",
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
		publisherOpts: options.PublisherOptions{
			Mandatory: true,
			Immediate: false,
		},
		ValueSerializer: NilSerializer{},
		Tracer:          otel.Tracer("bunny-producer"),
		TracePropagator: otel.GetTextMapPropagator(),
	}

	for _, option := range opts {
		option(rp)
	}

	rp.Ready = make(chan bool, 1)

	rp.client = rabbit.New(
		rp.connectionOpts,
		rp.exchangeOpts,
		rp.queueOpts,
		rp.Topic,
		&rp.publisherOpts,
		nil,
		rp.Ready,
	)

	rp.Metrics = metrics.NewProducerMetrics(rp.Topic)
	return rp, nil
}
