package options

import amqp "github.com/rabbitmq/amqp091-go"

type ExchangeOptions struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}
