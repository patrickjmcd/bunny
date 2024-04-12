package producer

import (
	"encoding/json"
	"errors"
	"google.golang.org/protobuf/encoding/protojson"

	"google.golang.org/protobuf/proto"
)

type ValueSerializer interface {
	Serialize(topic string, msg interface{}) ([]byte, error)
	GetContentType() string
	Close()
}

type NilSerializer struct{}

func (NilSerializer) Serialize(topic string, msg interface{}) ([]byte, error) {
	return nil, nil
}

func (NilSerializer) GetContentType() string {
	return ""
}

func (NilSerializer) Close() {}

type BytesSerializer struct{}

func (BytesSerializer) Serialize(topic string, msg interface{}) ([]byte, error) {
	if msg == nil {
		return nil, errors.New("message is nil")
	}
	msgBytes, ok := msg.([]byte)
	if !ok {
		return nil, errors.New("message is not a byte array")
	}
	return msgBytes, nil
}

func (BytesSerializer) GetContentType() string {
	return "application/octet-stream"
}

func (BytesSerializer) Close() {}

type ProtobufSerializer[T proto.Message] struct{}

func (ProtobufSerializer[T]) Serialize(topic string, msg interface{}) ([]byte, error) {
	return proto.Marshal(msg.(T))
}

func (ProtobufSerializer[T]) GetContentType() string {
	return "application/protobuf"
}

func (ProtobufSerializer[T]) Close() {}

type JsonSerializer[T any] struct{}

func (JsonSerializer[T]) Serialize(topic string, msg interface{}) ([]byte, error) {
	return json.Marshal(msg)
}

func (JsonSerializer[T]) GetContentType() string {
	return "application/json"
}

func (JsonSerializer[T]) Close() {}

// ProtoJsonSerializer is used to serialize protobuf messages to JSON
type ProtoJsonSerializer[T proto.Message] struct{}

func (ProtoJsonSerializer[T]) Serialize(topic string, msg interface{}) ([]byte, error) {
	return protojson.Marshal(msg.(T))
}

func (ProtoJsonSerializer[T]) GetContentType() string {
	return "application/json"
}

func (ProtoJsonSerializer[T]) Close() {}

func NewProtobufSerializer[T proto.Message]() (*ProtobufSerializer[T], error) {
	return &ProtobufSerializer[T]{}, nil
}

func NewJsonSerializer[T any]() (*JsonSerializer[T], error) {
	return &JsonSerializer[T]{}, nil
}

func NewProtoJsonSerializer[T proto.Message]() (*ProtoJsonSerializer[T], error) {
	return &ProtoJsonSerializer[T]{}, nil
}
