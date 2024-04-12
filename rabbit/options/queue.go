package options

import amqp "github.com/rabbitmq/amqp091-go"

type QueueOptions struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
	NoBind     bool
}
