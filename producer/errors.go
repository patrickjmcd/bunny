package producer

import "errors"

var (
	ErrProducerError = errors.New("failed to produce message")
	ErrSerialization = errors.New("failed to serialize message")
)
