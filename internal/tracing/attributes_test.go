package tracing

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
)

func TestAttributes_MessageHeaders(t *testing.T) {
	testCases := []struct {
		name     string
		msg      amqp.Table
		expected []attribute.KeyValue
	}{
		{
			name:     "No header",
			msg:      nil,
			expected: nil,
		},
		{
			name: "Single header",
			msg: amqp.Table{
				"header-1": []byte("value-1"),
			},
			expected: []attribute.KeyValue{
				{Key: "messaging.headers.header-1", Value: attribute.StringValue("value-1")},
			},
		},
		{
			name: "Multiple headers",
			msg: amqp.Table{
				"header-1": []byte("value-1"),
				"header-2": []byte("value-2"),
			},
			expected: []attribute.KeyValue{
				{Key: "messaging.headers.header-1", Value: attribute.StringValue("value-1")},
				{Key: "messaging.headers.header-2", Value: attribute.StringValue("value-2")},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			attributes := MessageHeaders(testCase.msg)
			assert.ElementsMatch(t, testCase.expected, attributes)
		})
	}
}
