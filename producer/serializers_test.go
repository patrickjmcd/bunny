package producer

import (
	"errors"
	"google.golang.org/protobuf/proto"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNilSerializer_Serialize(t *testing.T) {
	serializer := NilSerializer{}
	topic := "test"
	msg := "message"

	data, err := serializer.Serialize(topic, msg)

	assert.Nil(t, data)
	assert.NoError(t, err)
}

func TestBytesSerializer_Serialize(t *testing.T) {
	serializer := BytesSerializer{}
	topic := "test"
	msg := []byte("hello")

	result, err := serializer.Serialize(topic, msg)
	assert.NoError(t, err)
	assert.Equal(t, msg, result)
}

func TestBytesSerializer_Serialize_NilMessage(t *testing.T) {
	serializer := BytesSerializer{}
	topic := "test"
	var msg interface{} = nil

	_, err := serializer.Serialize(topic, msg)
	assert.Error(t, err)

	expectedError := errors.New("message is nil")
	assert.Equal(t, expectedError, err)
}

func TestJsonSerializer_Serialize(t *testing.T) {
	type testMessage struct {
		Name string `json:"name"`
		Age  int32  `json:"age"`
	}
	serializer := JsonSerializer[testMessage]{}
	topic := "test"
	msg := testMessage{
		Name: "test",
		Age:  30,
	}

	result, err := serializer.Serialize(topic, msg)
	assert.NoError(t, err)
	assert.Equal(t, `{"name":"test","age":30}`, string(result))
}

func TestNewProtobufValueSerializer_Success(t *testing.T) {
	type testMessage proto.Message
	s, err := NewProtobufSerializer[testMessage]()

	assert.NotNil(t, s)
	assert.NoError(t, err)
}

func TestNewJsonValueSerializer_Success(t *testing.T) {
	type testMessage struct {
		Name string `json:"name"`
		Age  int32  `json:"age"`
	}
	s, err := NewJsonSerializer[testMessage]()

	assert.NotNil(t, s)
	assert.NoError(t, err)
}
