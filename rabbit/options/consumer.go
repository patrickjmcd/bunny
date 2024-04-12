package options

import amqp "github.com/rabbitmq/amqp091-go"

type ConsumerOptions struct {
	QueueName     string
	Name          string
	AutoAck       bool
	Exclusive     bool
	NoLocal       bool
	NoWait        bool
	Args          amqp.Table
	PrefetchCount int
}
