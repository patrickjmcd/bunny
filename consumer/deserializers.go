package consumer

import (
	"context"
	"encoding/json"
	"reflect"

	"google.golang.org/protobuf/encoding/protojson"

	"google.golang.org/protobuf/proto"
)

type ValueDeserializer interface {
	Deserialize(ctx context.Context, topic string, payload []byte) (interface{}, error)
	DeserializeInto(ctx context.Context, topic string, payload []byte, msg interface{}) error
	Close()
}

type NilDeserializer struct{}

func (NilDeserializer) Deserialize(ctx context.Context, topic string, payload []byte) (interface{}, error) {
	return nil, nil
}

func (NilDeserializer) DeserializeInto(ctx context.Context, topic string, payload []byte, msg interface{}) error {
	return nil
}

func (NilDeserializer) Close() {}

type StringDeserializer struct{}

func (StringDeserializer) Deserialize(ctx context.Context, topic string, payload []byte) (interface{}, error) {
	return string(payload), nil
}

func (StringDeserializer) DeserializeInto(ctx context.Context, topic string, payload []byte, msg interface{}) error {
	return nil
}

func (StringDeserializer) Close() {}

type ProtobufDeserializer[T proto.Message] struct{}

func (ProtobufDeserializer[T]) Deserialize(ctx context.Context, topic string, payload []byte) (interface{}, error) {
	var msg T
	msgType := reflect.TypeOf(msg).Elem()
	msg = reflect.New(msgType).Interface().(T)
	errUnmarshal := proto.Unmarshal(payload, msg)
	return msg, errUnmarshal
}

func (ProtobufDeserializer[T]) DeserializeInto(ctx context.Context, topic string, payload []byte, msg interface{}) error {
	err := proto.Unmarshal(payload, msg.(T))
	if err != nil {
		return err
	}
	return nil
}

func (ProtobufDeserializer[T]) Close() {}

type JSONDeserializer[T any] struct{}

func (JSONDeserializer[T]) Deserialize(ctx context.Context, topic string, payload []byte) (interface{}, error) {
	var msg T
	err := json.Unmarshal(payload, &msg)
	return msg, err
}

func (JSONDeserializer[T]) DeserializeInto(ctx context.Context, topic string, payload []byte, msg interface{}) error {
	err := json.Unmarshal(payload, &msg)
	return err
}

func (JSONDeserializer[T]) Close() {}

type ProtoJsonDeserializer[T proto.Message] struct{}

func (ProtoJsonDeserializer[T]) Deserialize(ctx context.Context, topic string, payload []byte) (interface{}, error) {
	var msg T
	msgType := reflect.TypeOf(msg).Elem()
	msg = reflect.New(msgType).Interface().(T)
	err := protojson.Unmarshal(payload, msg)
	return msg, err
}

func (ProtoJsonDeserializer[T]) DeserializeInto(ctx context.Context, topic string, payload []byte, msg interface{}) error {
	err := protojson.Unmarshal(payload, msg.(T))
	return err
}

func (ProtoJsonDeserializer[T]) Close() {}

func NewProtobufValueDeserializer[T proto.Message]() (*ProtobufDeserializer[T], error) {
	return &ProtobufDeserializer[T]{}, nil
}

func NewJsonValueDeserializer[T any]() (*JSONDeserializer[T], error) {
	return &JSONDeserializer[T]{}, nil
}

func NewProtoJsonValueDeserializer[T proto.Message]() (*ProtoJsonDeserializer[T], error) {
	return &ProtoJsonDeserializer[T]{}, nil
}
