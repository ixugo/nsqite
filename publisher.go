package nsqite

import (
	"time"

	"github.com/google/uuid"
)

// Publisher 消息发布者
type Publisher[T any] struct{}

// NewPublisher 创建消息发布者
func NewPublisher[T any]() *Publisher[T] {
	return &Publisher[T]{}
}

// Publish 发布消息
func (p *Publisher[T]) Publish(topic string, msg T) {
	eventBus.Publish(topic, &EventMessage[T]{
		ID:        uuid.New().String(),
		Body:      msg,
		Timestamp: time.Now().UnixMilli(),
	})
}

// Stop 停止消息发布者
func (p *Publisher[T]) Stop() {
}
