package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type ConsumerMetrics struct {
	ReceiveLatency     prometheus.Histogram
	ProcessingDuration prometheus.Histogram
}

func NewConsumerMetrics(exchange, queue, topic string) *ConsumerMetrics {
	return &ConsumerMetrics{
		ReceiveLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:        "bunny_receive_latency_ms",
			ConstLabels: map[string]string{"exchange": exchange, "queue": queue, "topic": topic},
			Help:        "the latency of the consumer receiving a message from when it was published",
			Buckets:     prometheus.ExponentialBuckets(2, 2, 20),
		}),
		ProcessingDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:        "bunny_processing_duration_ms",
			ConstLabels: map[string]string{"exchange": exchange, "queue": queue, "topic": topic},
			Help:        "the time it takes to process a message",
			Buckets:     prometheus.ExponentialBuckets(2, 2, 20),
		}),
	}
}
