package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type ProducerMetrics struct {
	Total prometheus.Counter
}

func NewProducerMetrics(topic string) *ProducerMetrics {
	return &ProducerMetrics{
		Total: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "bunny_produced_total",
			Help:        "the total number of messages produced",
			ConstLabels: prometheus.Labels{"topic": topic},
		}),
	}
}
