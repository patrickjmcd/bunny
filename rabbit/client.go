package rabbit

import (
	"context"
	"github.com/patrickjmcd/bunny/rabbit/options"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"time"
)

const (
	reconnectDelay = 5 * time.Second
	reInitDelay    = 2 * time.Second
	resendDelay    = 5 * time.Second
)

type Client struct {
	connectionOpts options.ConnectionOptions
	exchangeOpts   options.ExchangeOptions
	queueOpts      options.QueueOptions
	publisherOpts  *options.PublisherOptions
	consumerOpts   *options.ConsumerOptions
	topic          string

	logger          zerolog.Logger
	connection      *amqp.Connection
	done            chan bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	isReady         bool

	channel  *amqp.Channel
	Delivery <-chan amqp.Delivery
	Ready    chan bool
}

// New creates a new consumer state instance, and automatically
// attempts to connect to the server.
func New(
	connectionOptions options.ConnectionOptions,
	exchangeOptions options.ExchangeOptions,
	queueOptions options.QueueOptions,
	topic string,
	publisherOptions *options.PublisherOptions,
	consumerOptions *options.ConsumerOptions,
	readyChan chan bool,
) *Client {
	client := Client{
		connectionOpts: connectionOptions,
		exchangeOpts:   exchangeOptions,
		queueOpts:      queueOptions,
		topic:          topic,
		Ready:          readyChan,
		isReady:        false,
		done:           make(chan bool),
	}
	if publisherOptions != nil {
		client.publisherOpts = publisherOptions
	}
	if consumerOptions != nil {
		client.consumerOpts = consumerOptions
	}

	if client.connectionOpts.ConnectionString == "" {
		client.connectionOpts.ConnectionString = client.connectionOpts.Protocol + "://" + client.connectionOpts.Username + ":" + client.connectionOpts.Password + "@" + client.connectionOpts.Host + ":" + client.connectionOpts.Port + client.connectionOpts.Vhost
	}

	go client.handleReconnect(client.connectionOpts.ConnectionString)

	client.logger = log.Logger.With().Str("exchange", exchangeOptions.Name).Str("queue", queueOptions.Name).Str("topic", topic).Logger()

	return &client
}

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (client *Client) handleReconnect(addr string) {
	for {
		client.isReady = false
		client.logger.Debug().Str("fn", "Client.handleReconnect").Str("connectionString", addr).Msg("Attempting to connect")

		conn, err := client.connect(addr)

		if err != nil {
			client.logger.Error().Err(err).Str("fn", "Client.handleReconnect").Msg("Failed to connect, retrying...")

			select {
			case <-client.done:
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}

		if done := client.handleReInit(conn); done {
			break
		}
	}

}

// connect will create a new AMQP connection
func (client *Client) connect(addr string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(addr)

	if err != nil {
		client.logger.Error().Err(err).Str("fn", "Client.connect").Msg("Failed to connect")
		return nil, ErrConnectionFailed
	}

	client.changeConnection(conn)
	client.logger.Debug().Msg("Connected!")
	return conn, nil
}

// handleReconnect will wait for a channel error
// and then continuously attempt to re-initialize both channels
func (client *Client) handleReInit(conn *amqp.Connection) bool {
	for {
		client.isReady = false

		err := client.init(conn)

		if err != nil {
			client.logger.Error().Str("fn", "Client.handleReInit").Err(err).Msg("Failed to initialize channel. Retrying...")

			select {
			case <-client.done:
				return true
			case <-client.notifyConnClose:
				client.logger.Error().Str("fn", "Client.handleReInit").Msg("Connection closed. Reconnecting...")
				return false
			case <-time.After(reInitDelay):
			}
			continue
		}

		select {
		case <-client.done:
			return true
		case <-client.notifyConnClose:
			client.logger.Error().Str("fn", "Client.handleReInit").Msg("Connection closed. Reconnecting...")
			return false
		case <-client.notifyChanClose:
			client.logger.Error().Str("fn", "Client.handleReInit").Msg("Channel closed. Re-running init...")
		}
	}
}

// init will initialize channel & declare queue
func (client *Client) init(conn *amqp.Connection) error {
	err := client.getChannel()
	if err != nil {
		return err // already formatted
	}

	client.isReady = true
	client.Ready <- true
	client.logger.Debug().Str("fn", "Client.init").Msg("rabbit client setup successful")

	return nil
}

func (client *Client) getChannel() error {
	conn, err := amqp.Dial(client.connectionOpts.ConnectionString)
	if err != nil {
		client.logger.Error().Err(err).Str("fn", "Client.getChannel").Msg("failed to create rabbit connection")
		return ErrConnectionFailed
	}
	ch, err := conn.Channel()
	if err != nil {
		client.logger.Error().Err(err).Str("fn", "Client.getChannel").Msg("failed to create rabbit channel")
		return err // already formatted
	}

	err = ch.Confirm(client.exchangeOpts.NoWait)
	if err != nil {
		client.logger.Error().Err(err).Str("fn", "Client.getChannel").Msg("failed to set confirm mode")
		return ErrCannotConfirmChannel
	}
	if client.exchangeOpts.Name != "" {
		err = ch.ExchangeDeclare(
			client.exchangeOpts.Name,       // name
			client.exchangeOpts.Type,       // type
			client.exchangeOpts.Durable,    // durable
			client.exchangeOpts.AutoDelete, // auto-deleted
			client.exchangeOpts.Internal,   // internal
			client.exchangeOpts.NoWait,     // no-wait
			client.exchangeOpts.Args,       // arguments
		)
		if err != nil {
			client.logger.Error().Err(err).Str("fn", "Client.getChannel").Msg("failed to declare exchange")
			return ErrCannotDeclareExchange
		}
	}
	q, err := ch.QueueDeclare(
		client.queueOpts.Name,       // name
		client.queueOpts.Durable,    // durable
		client.queueOpts.AutoDelete, // delete when unused
		client.queueOpts.Exclusive,  // exclusive
		client.queueOpts.NoWait,     // no-wait
		client.queueOpts.Args,       // arguments
	)
	if err != nil {
		client.logger.Error().Err(err).Str("fn", "Client.getChannel").Msg("failed to declare queue")
		return ErrCannotDeclareQueue
	}

	// can't bind to default exchange
	if client.exchangeOpts.Name != "" && client.topic != "" && !client.queueOpts.NoBind {
		err := ch.QueueBind(
			client.queueOpts.Name,    // queue name
			client.topic,             // routing key
			client.exchangeOpts.Name, // exchange
			client.queueOpts.NoWait,  // no-wait
			client.queueOpts.Args,    // args
		)
		if err != nil {
			client.logger.Error().Err(err).Str("fn", "Client.Consume").Msg("failed to bind queue")
			return ErrCannotBindQueue
		}
	}

	if client.queueOpts.Name == "" {
		client.queueOpts.Name = q.Name
	}
	client.changeChannel(ch)
	return nil
}

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (client *Client) changeConnection(connection *amqp.Connection) {
	client.connection = connection
	client.notifyConnClose = make(chan *amqp.Error, 1)
	client.connection.NotifyClose(client.notifyConnClose)
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (client *Client) changeChannel(channel *amqp.Channel) {
	client.channel = channel
	client.notifyChanClose = make(chan *amqp.Error, 1)
	client.notifyConfirm = make(chan amqp.Confirmation, 1)
	client.channel.NotifyClose(client.notifyChanClose)
	client.channel.NotifyPublish(client.notifyConfirm)
}

// Push will push data onto the queue, and wait for a confirm.
// This will block until the server sends a confirm. Errors are
// only returned if the push action itself fails, see UnsafePush.
func (client *Client) Push(message amqp.Publishing) error {
	if !client.isReady {
		client.logger.Error().Str("fn", "Client.Push").Msg("failed to push: not connected")
		return ErrNotConnected
	}

	if message.Timestamp.IsZero() {
		message.Timestamp = time.Now()
	}

	for {
		err := client.UnsafePush(message)
		if err != nil {
			client.logger.Error().Err(err).Str("fn", "Client.Push").Msg("push failed, retrying...")
			select {
			case <-client.done:
				return ErrShutdown
			case <-time.After(resendDelay):
			}
			continue
		}
		confirm := <-client.notifyConfirm
		if confirm.Ack {
			client.logger.Debug().Str("correlationID", message.CorrelationId).Msgf("push confirmed! deliveryTag: %d", confirm.DeliveryTag)
			return nil
		}
	}
}

// UnsafePush will push to the queue without checking for
// confirmation. It returns an error if it fails to connect.
// No guarantees are provided for whether the server will
// receive the message.
func (client *Client) UnsafePush(message amqp.Publishing) error {
	if !client.isReady {
		client.logger.Error().Str("fn", "Client.UnsafePush").Msg("failed to push: not connected")
		return ErrNotConnected
	}

	if client.publisherOpts == nil {
		client.logger.Error().Str("fn", "Client.UnsafePush").Msg("publisher options not set")
		return ErrPublisherOptionsNotSet
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return client.channel.PublishWithContext(
		ctx,
		client.exchangeOpts.Name,
		client.topic,
		client.publisherOpts.Mandatory,
		client.publisherOpts.Immediate,
		message,
	)
}

// Consume will continuously put queue items on the channel.
// It is required to call delivery.Ack when it has been
// successfully processed, or delivery.Nack when it fails.
// Ignoring this will cause data to build up on the server.
func (client *Client) Consume() (<-chan amqp.Delivery, error) {
	if !client.isReady {
		return nil, ErrNotConnected
	}

	if client.consumerOpts == nil {
		client.logger.Error().Str("fn", "Client.Consume").Msg("consumer options not set")
		return nil, ErrConsumerOptionsNotSet
	}

	if client.channel == nil {
		client.logger.Error().Str("fn", "Client.Consume").Msg("channel not initialized")
		return nil, ErrChannelNotInitialized
	}

	// can't bind to default exchange
	if client.exchangeOpts.Name != "" && client.topic != "" && !client.queueOpts.NoBind {
		err := client.channel.QueueBind(
			client.queueOpts.Name,    // queue name
			client.topic,             // routing key
			client.exchangeOpts.Name, // exchange
			client.queueOpts.NoWait,  // no-wait
			client.queueOpts.Args,    // args
		)
		if err != nil {
			client.logger.Error().Err(err).Str("fn", "Client.Consume").Msg("failed to bind queue")
			return nil, ErrCannotBindQueue
		}
	}
	if client.consumerOpts.PrefetchCount > 0 {
		if err := client.channel.Qos(
			client.consumerOpts.PrefetchCount,
			0,
			false,
		); err != nil {
			client.logger.Error().Err(err).Str("fn", "Client.Consume").Msg("failed to set QoS")
			return nil, ErrFailedToSetPrefetchCount
		}
	}

	return client.channel.Consume(
		client.queueOpts.Name,         // queue
		client.consumerOpts.Name,      // consumer
		client.consumerOpts.AutoAck,   // auto-ack
		client.consumerOpts.Exclusive, // exclusive
		client.consumerOpts.NoLocal,   // no-local
		client.consumerOpts.NoWait,    // no-wait
		client.consumerOpts.Args,      // args
	)
}

// Close will cleanly shut down the channel and connection.
func (client *Client) Close() error {
	if !client.isReady {
		client.logger.Error().Str("fn", "Client.Close").Msg("failed to close: not connected")
		return ErrAlreadyClosed
	}
	close(client.done)
	err := client.channel.Close()
	if err != nil {
		client.logger.Error().Err(err).Str("fn", "Client.Close").Msg("failed to close channel")
		return ErrChannelCloseFailed
	}
	err = client.connection.Close()
	if err != nil {
		client.logger.Error().Err(err).Str("fn", "Client.Close").Msg("failed to close connection")
		return ErrConnectionCloseFailed
	}

	client.isReady = false
	return nil
}

// SetNotifyClose will set the notify close channel
func (client *Client) SetNotifyClose(notifyClose chan *amqp.Error) {
	client.channel.NotifyClose(notifyClose)
}
