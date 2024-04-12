package consumer

import "errors"

var (
	ErrConsumerClose             = errors.New("failed to close rabbitmq consumer")
	ErrValueDeserialization      = errors.New("failed to deserialize value")
	ErrMessageHandler            = errors.New("message handler failed")
	ErrAck                       = errors.New("failed to acknowledge message")
	ErrConsumerRetryLimitReached = errors.New("consumer retry limit reached")
)

// these errors are the result of a rabbitmq failure and should be considered fatal/critical
var (
	ErrMessageErrorHandler = errors.New("message error handler failed processing")
)
