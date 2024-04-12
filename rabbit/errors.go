package rabbit

import "errors"

var (
	ErrNotConnected             = errors.New("not connected to a server")
	ErrAlreadyClosed            = errors.New("already closed: not connected to the server")
	ErrShutdown                 = errors.New("client is shutting down")
	ErrConnectionFailed         = errors.New("failed to connect to rabbitmq server")
	ErrCannotConfirmChannel     = errors.New("cannot set confirm mode")
	ErrCannotDeclareExchange    = errors.New("cannot declare exchange")
	ErrCannotDeclareQueue       = errors.New("cannot declare queue")
	ErrCannotBindQueue          = errors.New("cannot bind queue")
	ErrPublisherOptionsNotSet   = errors.New("publisher options not set")
	ErrConsumerOptionsNotSet    = errors.New("consumer options not set")
	ErrChannelNotInitialized    = errors.New("channel not initialized")
	ErrChannelCloseFailed       = errors.New("failed to close channel")
	ErrConnectionCloseFailed    = errors.New("failed to close connection")
	ErrFailedToSetPrefetchCount = errors.New("failed to set prefetch count")
)
