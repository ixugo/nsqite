package nsqite

import (
	"fmt"
	"testing"
	"time"
)

type TestHandler struct{}

func (h *TestHandler) HandleMessage(message *Message) error {
	fmt.Println(string(message.Body))
	return nil
}

func TestSimple(t *testing.T) {
	producter := NewProducter(100)
	consumer := NewConsumer("test", "c1")
	consumer.AddConcurrentHandlers(&TestHandler{}, 1)

	producter.Publish("test", []byte("hello"))
	time.Sleep(1 * time.Second)
}
