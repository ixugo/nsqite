package nsqite

import (
	"log/slog"
	"sync/atomic"
	"time"
)

// Message 表示一条消息
type Message struct {
	ID        string
	TopicID   int64
	Body      []byte
	CreatedAt time.Time
	StartTime time.Time     // 消息开始处理的时间
	Timeout   time.Duration // 消息超时时间
	Delegate  MessageDelegate

	responded            int32 // 是否已得到处理
	autoResponseDisabled int32 // 是否禁止自动完成

	topic string // 消息所属的 topic
}

// MessageDelegate 消息代理接口
type MessageDelegate interface {
	OnFinish(message *Message)
	OnRequeue(message *Message, delay time.Duration)
	OnTouch(message *Message)
}

type Handler interface {
	HandleMessage(message *Message) error
}

// MDelegate 默认消息代理实现
type MDelegate struct{}

// OnFinish 消息完成处理
func (d *MDelegate) OnFinish(message *Message) {
	slog.Debug("消息处理完成", "messageID", message.ID)
}

// OnRequeue 消息重新入队处理
func (d *MDelegate) OnRequeue(message *Message, delay time.Duration) {
	slog.Debug("消息重新入队", "messageID", message.ID, "delay", delay)
}

// OnTouch 消息触摸处理
func (d *MDelegate) OnTouch(message *Message) {
	slog.Debug("消息被触摸", "messageID", message.ID)
}

var _ MessageDelegate = &MDelegate{}

// Finish 消息处理完成
func (m *Message) Finish() {
	if !atomic.CompareAndSwapInt32(&m.responded, 0, 1) {
		return
	}
	m.Delegate.OnFinish(m)
}

// Requeue 重新入队
func (m *Message) Requeue(delay time.Duration) {
	if !atomic.CompareAndSwapInt32(&m.responded, 0, 1) {
		return
	}
	m.Delegate.OnRequeue(m, delay)
}

// Touch 消息处理中
func (m *Message) Touch() {
	m.Delegate.OnTouch(m)
}

// HasResponded 消息是否已得到处理
func (m *Message) HasResponded() bool {
	return atomic.LoadInt32(&m.responded) == 1
}

// DisableAutoResponse 禁止自动完成
func (m *Message) DisableAutoResponse() {
	atomic.StoreInt32(&m.autoResponseDisabled, 1)
}

// EnableAutoResponse 允许自动完成
func (m *Message) EnableAutoResponse() {
	atomic.StoreInt32(&m.autoResponseDisabled, 0)
}

// IsAutoResponseDisabled 是否禁止自动完成
func (m *Message) IsAutoResponseDisabled() bool {
	return atomic.LoadInt32(&m.autoResponseDisabled) == 1
}
