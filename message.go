package nsqite

import (
	"sync/atomic"
	"time"
)

// Message 表示一条消息
type Message struct {
	ID        int       `gorm:"primaryKey" json:"id"`
	Timestamp time.Time `gorm:"column:timestamp;notNull;default:CURRENT_TIMESTAMP;index;comment:创建时间" json:"timestamp"` // 创建时间
	Topic     string    `gorm:"notNull;index;default:''" json:"topic"`                                                  // 消息所属的 topic
	Body      []byte    `gorm:"notNull" json:"body"`                                                                    // 消息内容
	// 订阅数和完成数
	Consumers         uint32 `gorm:"notNull;default:0;index:idx_messages_consumers_responded" json:"consumers"` // 消息订阅数
	Responded         uint32 `gorm:"notNull;default:0;index:idx_messages_consumers_responded" json:"responded"` // 消息响应完成数
	Channels          string `gorm:"notNull;default:''" json:"channels"`                                        // 消息订阅通道
	RespondedChannels string `gorm:"notNull;default:''" json:"responded_channels"`                              // 消息响应完成通道
	Attempts          uint32 `gorm:"notNull;default:0" json:"attempts"`                                         // 消息重试次数

	responded int32 // 是否已得到处理
	// runtime
	Delegate             MessageDelegate `gorm:"-" json:"-"`
	autoResponseDisabled int32           // 是否禁止自动完成，1表示禁用，0表示启用
}

func (*Message) TableName() string {
	return "nsqite_messages"
}

// MessageDelegate 消息代理接口
type MessageDelegate interface {
	OnFinish(message *Message)
	OnRequeue(message *Message, delay time.Duration)
	OnTouch(message *Message)
}

// Finish 消息处理完成
func (m *Message) Finish() {
	if !atomic.CompareAndSwapInt32(&m.responded, 0, 1) {
		return
	}
	m.Delegate.OnFinish(m)
}

// Requeue 重新入队
func (m *Message) Requeue(delay time.Duration) {
	if atomic.LoadInt32(&m.responded) == 1 {
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
