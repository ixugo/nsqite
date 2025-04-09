package nsqite

import (
	"sync/atomic"
	"time"
)

type EventMessage[T any] struct {
	ID        string
	Body      T
	Timestamp int64
	Attempts  uint32

	Delegate EventMessageDelegate[T]

	autoResponseDisabled int32 // 是否禁止自动完成
	responded            int32 // 消息是否已得到处理
}

// MessageDelegate is an interface of methods that are used as
// callbacks in Message
type EventMessageDelegate[T any] interface {
	// OnFinish is called when the Finish() method
	// is triggered on the Message
	OnFinish(*EventMessage[T])

	// OnRequeue is called when the Requeue() method
	// is triggered on the Message
	OnRequeue(m *EventMessage[T], delay time.Duration)

	// OnTouch is called when the Touch() method
	// is triggered on the Message
	// 超时应该由消息订阅者来处理
	OnTouch(*EventMessage[T])
}

type EventHandler[T any] interface {
	HandleMessage(message *EventMessage[T]) error
}

// Finish 消息处理完成
func (m *EventMessage[T]) Finish() {
	if !atomic.CompareAndSwapInt32(&m.responded, 0, 1) {
		return
	}
	m.Delegate.OnFinish(m)
}

// HasResponded 消息是否已得到处理，true 表示已得到处理
func (m *EventMessage[T]) HasResponded() bool {
	return atomic.LoadInt32(&m.responded) == 1
}

// IsAutoResponseDisabled 是否禁止自动完成, true 表示禁止
func (m *EventMessage[T]) IsAutoResponseDisabled() bool {
	return atomic.LoadInt32(&m.autoResponseDisabled) == 1
}

// DisableAutoResponse 禁用自动完成
func (m *EventMessage[T]) DisableAutoResponse() {
	atomic.StoreInt32(&m.autoResponseDisabled, 1)
}

// Requeue 重新入队
func (m *EventMessage[T]) Requeue(delay time.Duration) {
	if atomic.LoadInt32(&m.responded) == 1 {
		return
	}
	m.Delegate.OnRequeue(m, delay)
}

// Touch 防止消息处理超时
func (m *EventMessage[T]) Touch() {
	if m.HasResponded() {
		return
	}
	m.Delegate.OnTouch(m)
}
