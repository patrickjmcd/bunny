package retry

import "errors"

var (
	ErrFailedToProduceRetryMessage = errors.New("failed to produce retry message")
	ErrFailedToAckMessage          = errors.New("failed to ack message")
)
