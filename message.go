package nsqite

import (
	"sync/atomic"
	"time"
)

type Message struct {
	ID        string
	Body      []byte
	Timestamp int64
	Attempts  uint16

	Delegate MessageDelegate

	autoResponseDisabled int32 // 是否禁止自动完成
	responded            int32 // 消息是否已得到处理
}

// MessageDelegate is an interface of methods that are used as
// callbacks in Message
type MessageDelegate interface {
	// OnFinish is called when the Finish() method
	// is triggered on the Message
	OnFinish(*Message)

	// OnRequeue is called when the Requeue() method
	// is triggered on the Message
	OnRequeue(m *Message, delay time.Duration, backoff bool)

	// OnTouch is called when the Touch() method
	// is triggered on the Message
	OnTouch(*Message)
}

type Handler interface {
	HandleMessage(message *Message) error
}

type MDelegate struct{}

// OnFinish implements MessageDelegate.
func (m *MDelegate) OnFinish(*Message) {
}

// OnRequeue implements MessageDelegate.
func (*MDelegate) OnRequeue(m *Message, delay time.Duration, backoff bool) {
}

// OnTouch implements MessageDelegate.
func (m *MDelegate) OnTouch(*Message) {
}

var _ MessageDelegate = &MDelegate{}

// Finish 消息处理完成
func (m *Message) Finish() {
	if !atomic.CompareAndSwapInt32(&m.responded, 0, 1) {
		return
	}
	m.Delegate.OnFinish(m)
}

// HasResponded 消息是否已得到处理，true 表示已得到处理
func (m *Message) HasResponded() bool {
	return atomic.LoadInt32(&m.responded) == 1
}

// IsAutoResponseDisabled 是否禁止自动完成, true 表示禁止
func (m *Message) IsAutoResponseDisabled() bool {
	return atomic.LoadInt32(&m.autoResponseDisabled) == 1
}

// Requeue 重新入队
func (m *Message) Requeue(delay time.Duration) {
	if !atomic.CompareAndSwapInt32(&m.responded, 0, 1) {
		return
	}
	m.Delegate.OnRequeue(m, delay, true)
}
