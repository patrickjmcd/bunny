package tracing

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/propagation"
)

func TestNewProducerMessageCarrier(t *testing.T) {
	msg := &amqp.Publishing{}

	carrier := NewProducerMessageCarrier(msg)

	assert.Implements(t, (*propagation.TextMapCarrier)(nil), carrier)
	assert.Equal(t, msg, carrier.msg)
}

func TestProducerMessageCarrier_Get(t *testing.T) {
	testCases := []struct {
		name     string
		msg      *amqp.Publishing
		key      string
		expected string
	}{
		{
			name:     "headers is not defined",
			msg:      &amqp.Publishing{},
			key:      "taceparent",
			expected: "",
		},
		{
			name: "value is absent",
			msg: &amqp.Publishing{
				Headers: amqp.Table{},
			},
			key:      "taceparent",
			expected: "",
		},
		{
			name: "value is present",
			msg: &amqp.Publishing{
				Headers: amqp.Table{
					"traceparent": []byte("123456789123456789123456789123-1234567891234567"),
				},
			},
			key:      "traceparent",
			expected: "123456789123456789123456789123-1234567891234567",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			carrier := NewProducerMessageCarrier(testCase.msg)
			value := carrier.Get(testCase.key)

			assert.Equal(t, testCase.expected, value)
		})
	}
}

func TestProducerMessageCarrier_Set(t *testing.T) {
	testCases := []struct {
		name     string
		msg      *amqp.Publishing
		key      string
		value    string
		expected amqp.Table
	}{
		{
			name:     "empty values",
			msg:      &amqp.Publishing{},
			key:      "",
			value:    "",
			expected: nil,
		},
		{
			name:  "set a new value",
			msg:   &amqp.Publishing{},
			key:   "traceparent",
			value: "123456789123456789123456789123-1234567891234567",
			expected: amqp.Table{
				"traceparent": "123456789123456789123456789123-1234567891234567",
			},
		},
		{
			name: "set a new value with other values set",
			msg: &amqp.Publishing{
				Headers: amqp.Table{
					"my-test-key": "my-value-key",
				},
			},
			key:   "traceparent",
			value: "123456789123456789123456789123-1234567891234567",
			expected: amqp.Table{
				"my-test-key": "my-value-key",
				"traceparent": "123456789123456789123456789123-1234567891234567",
			},
		},
		{
			name: "replace value when already defined",
			msg: &amqp.Publishing{
				Headers: amqp.Table{
					"traceparent": "an-old-value-that-should-be-overriden",
				},
			},
			key:   "traceparent",
			value: "123456789123456789123456789123-1234567891234567",
			expected: amqp.Table{
				"traceparent": "123456789123456789123456789123-1234567891234567",
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			carrier := NewProducerMessageCarrier(testCase.msg)
			carrier.Set(testCase.key, testCase.value)

			assert.Equal(t, testCase.expected, testCase.msg.Headers)
		})
	}
}

func TestProducerMessageCarrier_Keys(t *testing.T) {
	testCases := []struct {
		name     string
		msg      *amqp.Publishing
		expected []string
	}{
		{
			name:     "no headers defined",
			msg:      &amqp.Publishing{},
			expected: []string(nil),
		},
		{
			name: "empty values",
			msg: &amqp.Publishing{
				Headers: amqp.Table{},
			},
			expected: []string(nil),
		},
		{
			name: "having some values",
			msg: &amqp.Publishing{
				Headers: amqp.Table{
					"traceparent": []byte("123456789123456789123456789123-1234567891234567"),
					"my-test-key": []byte("my-test-value"),
				},
			},
			expected: []string{"traceparent", "my-test-key"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			carrier := NewProducerMessageCarrier(testCase.msg)
			assert.ElementsMatch(t, testCase.expected, carrier.Keys())
		})
	}
}
