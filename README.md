# Bunny

A Go library that provides a high-level wrapper around RabbitMQ (AMQP) with built-in OpenTelemetry tracing, Prometheus metrics, and flexible message serialization.

## Features

- **Producer and Consumer** abstractions over `amqp091-go`
- **Automatic reconnection** with backoff on connection/channel loss
- **Publisher confirms** to ensure delivery before returning
- **OpenTelemetry tracing** with trace context propagated via AMQP headers
- **Prometheus metrics** for message counts and processing latency
- **Pluggable serializers/deserializers**: JSON, Protocol Buffers, ProtoJSON, raw bytes, or custom
- **Retry handling** via a built-in `RetryPublisher` error handler
- **Functional options** pattern for clean, composable configuration

## Requirements

- Go 1.22.2+
- A running RabbitMQ instance (default: `localhost:5672`, credentials `guest`/`guest`)

## Installation

```bash
go get github.com/patrickjmcd/bunny
```

## Usage

### Producer

```go
import (
    "context"
    "github.com/patrickjmcd/bunny/producer"
)

p, err := producer.NewRabbitProducer(
    producer.WithConnectionString("amqp://guest:guest@localhost:5672/"),
    producer.WithExchangeName("my-exchange"),
    producer.WithExchangeType("topic"),
    producer.WithQueueName("my-queue"),
    producer.WithTopic("my.routing.key"),
    producer.WithValueSerializer(producer.NewJsonSerializer[MyMessage]()),
)
if err != nil {
    log.Fatal(err)
}
defer p.Close()

// Wait until the connection is ready
<-p.Ready

err = p.ProduceMessage(context.Background(), "correlation-id", MyMessage{...}, "")
```

To publish a raw AMQP message:

```go
import amqp "github.com/rabbitmq/amqp091-go"

err = p.ProduceRaw(ctx, "correlation-id", &amqp.Publishing{
    ContentType: "application/json",
    Body:        []byte(`{"hello":"world"}`),
})
```

### Consumer

```go
import (
    "context"
    "github.com/patrickjmcd/bunny/consumer"
    amqp "github.com/rabbitmq/amqp091-go"
)

type MyHandler struct{}

func (h *MyHandler) OnReceive(ctx context.Context, key, value interface{}) error {
    msg := value.(MyMessage)
    // process msg...
    return nil
}

type MyErrorHandler struct{}

func (h *MyErrorHandler) OnError(ctx context.Context, raw *amqp.Delivery) error {
    // handle or dead-letter the message
    return nil
}

c, err := consumer.NewRabbitConsumer(
    consumer.WithConnectionString("amqp://guest:guest@localhost:5672/"),
    consumer.WithExchangeName("my-exchange"),
    consumer.WithExchangeType("topic"),
    consumer.WithQueueName("my-queue"),
    consumer.WithTopic("my.routing.key"),
    consumer.WithValueDeserializer(consumer.NewJSONDeserializer[MyMessage]()),
    consumer.WithMessageHandler(&MyHandler{}),
    consumer.WithMessageErrorHandler(&MyErrorHandler{}),
    consumer.WithPrefetchCount(10),
)
if err != nil {
    log.Fatal(err)
}
defer c.Close()

// Blocks until ctx is cancelled or a fatal error occurs
if err := c.Run(context.Background()); err != nil {
    log.Fatal(err)
}
```

### Retry Publisher

Use `RetryPublisher` as the `MessageErrorHandler` to automatically re-publish failed messages to another exchange/queue.

```go
import (
    "github.com/patrickjmcd/bunny/retry"
    "github.com/patrickjmcd/bunny/producer"
)

retryProducer, _ := producer.NewRabbitProducer(
    producer.WithConnectionString("amqp://guest:guest@localhost:5672/"),
    producer.WithExchangeName("retry-exchange"),
    producer.WithQueueName("retry-queue"),
    producer.WithTopic("retry.key"),
)

retryHandler := retry.NewRetryPublisherConfig(
    retry.WithMessagePublisher(retryProducer),
)

c, _ := consumer.NewRabbitConsumer(
    // ...other options...
    consumer.WithMessageErrorHandler(retryHandler),
)
```

## Configuration Options

### Connection

| Option | Description | Default |
|--------|-------------|---------|
| `WithConnectionString(s)` | Full AMQP URI (overrides individual fields) | — |
| `WithProtocol(s)` | Protocol (`amqp` or `amqps`) | `amqp` |
| `WithHost(s)` | Broker hostname | `localhost` |
| `WithPort(s)` | Broker port | `5672` |
| `WithUsername(s)` | Username | `guest` |
| `WithVHost(s)` | Virtual host | `/` |

### Exchange

| Option | Description | Default |
|--------|-------------|---------|
| `WithExchangeName(s)` | Exchange name | `""` |
| `WithExchangeType(s)` | Type: `direct`, `fanout`, `topic`, `headers` | `""` |
| `WithExchangeDurable(b)` | Survive broker restart | `false` |
| `WithExchangeAutoDelete(b)` | Delete when unused | `false` |
| `WithExchangeInternal(b)` | Internal exchange | `false` |
| `WithExchangeNoWait(b)` | Do not wait for confirmation | `false` |
| `WithExchangeArgs(t)` | Additional arguments | `nil` |

### Queue

| Option | Description | Default |
|--------|-------------|---------|
| `WithQueueName(s)` | Queue name | `""` |
| `WithQueueDurable(b)` | Survive broker restart | `false` |
| `WithQueueAutoDelete(b)` | Delete when unused | `true` |
| `WithQueueExclusive(b)` | Exclusive to connection | `false` |
| `WithQueueNoWait(b)` | Do not wait for confirmation | `false` |
| `WithQueueNoBind(b)` | Skip exchange binding | `false` |
| `WithQueueArgs(t)` | Additional arguments | `nil` |

### Producer-specific

| Option | Description | Default |
|--------|-------------|---------|
| `WithTopic(s)` | Routing key for published messages | `"unconfigured-topic"` |
| `WithValueSerializer(v)` | Serializer implementation | `NilSerializer` |
| `WithPublisherMandatory(b)` | Return unroutable messages | `true` |
| `WithPublisherImmediate(b)` | Fail if no immediate consumer | `false` |
| `WithTracer(t)` | OpenTelemetry tracer | global OTEL tracer |
| `WithTracePropagator(p)` | OTEL text map propagator | global propagator |

### Consumer-specific

| Option | Description | Default |
|--------|-------------|---------|
| `WithTopic(s)` | Routing key to bind | `"unconfigured-Topic"` |
| `WithValueDeserializer(v)` | Deserializer implementation | `NilDeserializer` |
| `WithMessageHandler(h)` | Handler for successfully deserialized messages | `NoOpMessageHandler` |
| `WithMessageErrorHandler(h)` | Handler for failed messages | `NoOpMessageErrorHandler` |
| `WithProcessingDelay(d)` | Artificial delay before processing each message | `0` |
| `WithConsumerName(s)` | Consumer tag | `""` |
| `WithConsumerAutoAck(b)` | Acknowledge messages automatically | `false` |
| `WithConsumerExclusive(b)` | Exclusive consumer | `false` |
| `WithPrefetchCount(n)` | QoS prefetch count | `0` (unlimited) |
| `WithSuppressProcessingErrors(b)` | Suppress handler error logs | `false` |
| `WithTracer(t)` | OpenTelemetry tracer | noop tracer |
| `WithTracePropagator(p)` | OTEL text map propagator | TraceContext + Baggage |

## Serializers and Deserializers

### Producer serializers (`producer` package)

| Type | Description |
|------|-------------|
| `NilSerializer` | No-op, passes body as-is |
| `BytesSerializer` | Raw `[]byte` |
| `JsonSerializer[T]` | JSON encoding of type `T` |
| `ProtobufSerializer[T]` | Protocol Buffer binary encoding |
| `ProtoJsonSerializer[T]` | Protocol Buffer JSON encoding |

Implement the `ValueSerializer` interface for custom formats:

```go
type ValueSerializer interface {
    Serialize(topic string, value interface{}) ([]byte, error)
    GetContentType() string
    Close()
}
```

### Consumer deserializers (`consumer` package)

| Type | Description |
|------|-------------|
| `NilDeserializer` | No-op, returns body as-is |
| `StringDeserializer` | Converts body to `string` |
| `JSONDeserializer[T]` | JSON decoding into type `T` |
| `ProtobufDeserializer[T]` | Protocol Buffer binary decoding |
| `ProtoJsonDeserializer[T]` | Protocol Buffer JSON decoding |

Implement `ValueDeserializer` for custom formats:

```go
type ValueDeserializer interface {
    Deserialize(ctx context.Context, topic string, data []byte) (interface{}, error)
    DeserializeInto(ctx context.Context, topic string, data []byte, dest interface{}) error
    Close()
}
```

## Observability

### OpenTelemetry

Bunny creates spans for each produce and consume operation, propagating trace context through AMQP message headers. Pass your configured tracer and propagator via `WithTracer` and `WithTracePropagator`.

### Prometheus Metrics

The following metrics are registered automatically:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `bunny_produced_total` | Counter | `topic` | Total messages produced |
| `bunny_receive_latency_ms` | Histogram | `exchange`, `queue`, `topic` | Time from message timestamp to receipt |
| `bunny_processing_duration_ms` | Histogram | `exchange`, `queue`, `topic` | Time to process each message |

## Packages

| Package | Description |
|---------|-------------|
| `rabbit` | Low-level AMQP client with reconnection logic |
| `rabbit/options` | Connection, exchange, queue, consumer, and publisher option types |
| `producer` | High-level message publisher |
| `consumer` | High-level message consumer |
| `retry` | `MessageErrorHandler` implementation that re-publishes failed messages |

## License

MIT
