package producer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"
)

func Test_WithTopic(t *testing.T) {
	srp := &RabbitProducer{}
	WithTopic("test")(srp)

	assert.Equal(t, "test", srp.Topic)
}

func Test_WithValueSerializer(t *testing.T) {
	mockSerializer := &NilSerializer{}
	srp := &RabbitProducer{}

	WithValueSerializer(mockSerializer)(srp)

	assert.Equal(t, mockSerializer, srp.ValueSerializer)
}

func TestWithTracer(t *testing.T) {
	mockTracer := noop.NewTracerProvider().Tracer("test")

	srp := &RabbitProducer{}

	WithTracer(mockTracer)(srp)

	assert.Equal(t, mockTracer, srp.Tracer)
}

func TestWithTracePropagator(t *testing.T) {
	propagator := propagation.NewCompositeTextMapPropagator()

	srp := &RabbitProducer{}
	option := WithTracePropagator(propagator)
	option(srp)

	assert.Equal(t, propagator, srp.TracePropagator)
}
