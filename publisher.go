package nsqite

import (
	"time"

	"github.com/google/uuid"
)

type Publisher[T any] struct{}

func NewPublisher[T any]() *Publisher[T] {
	return &Publisher[T]{}
}

func (p *Publisher[T]) Publish(topic string, msg T) {
	eventBus.Publish(topic, &EventMessage[T]{
		ID:        uuid.New().String(),
		Body:      msg,
		Timestamp: time.Now().UnixMilli(),
	})
}

func (p *Publisher[T]) Stop() {
}
