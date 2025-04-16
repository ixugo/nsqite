package nsqite

import (
	"context"
	"sync/atomic"
	"time"
)

var guid uint64

// Publisher 消息发布者
type Publisher[T any] struct{}

// NewPublisher 创建消息发布者
func NewPublisher[T any]() *Publisher[T] {
	return &Publisher[T]{}
}

// Publish 发布消息
// 此函数会返回 err，正常使用发布订阅不会出错，可以直接丢弃 err 不处理
// 如果使用 PublishWithContext 则需要处理 err
// 有哪些情况会触发 err 呢?
// 1. 没有订阅时发布
// 2. 使用 PublishWithContext 发布超时，订阅者没有足够的能力快速处理任务
func (p *Publisher[T]) Publish(topic string, msg T) error {
	return eventBus.Publish(context.Background(), topic, &EventMessage[T]{
		ID:        atomic.AddUint64(&guid, 1),
		Body:      msg,
		Timestamp: time.Now().UnixMilli(),
	})
}

// PublishWithContext 发布限时消息
// 如果需要限制发布超时，请使用此函数，可以根据返回的 err 判断是否发布超时
func (p *Publisher[T]) PublishWithContext(ctx context.Context, topic string, msg T) error {
	return eventBus.Publish(ctx, topic, &EventMessage[T]{
		ID:        atomic.AddUint64(&guid, 1),
		Body:      msg,
		Timestamp: time.Now().UnixMilli(),
	})
}
