package storage

import (
	"time"
)

// Storage 定义了持久化接口
type Storage interface {
	// 初始化存储
	Init(config interface{}) error

	// 关闭存储连接
	Close() error

	// Channel相关操作
	AddChannel(channel *Channel) error
	GetChannel(topicName, channelName string) (*Channel, error)
	FindChannels(topicName string) ([]*Channel, error)
	EditChannel(channel *Channel) error
	DelChannel(topicName, channelName string) error

	// Message相关操作
	SaveMessage(message *Message) error
	GetMessage(id string) (*Message, error)
	FindMessages(topicName string, limit, offset int) ([]*Message, error)

	// ChannelMessage相关操作
	SaveChannelMessage(channelMessage *ChannelMessage) error
	GetChannelMessage(channelID int, messageID string) (*ChannelMessage, error)
	FindChannelMessages(channelID int, delivered, finished, requeued int, limit, offset int) ([]*ChannelMessage, error)
	EditChannelMessage(channelMessage *ChannelMessage) error

	// 消息订阅相关操作
	Subscribe(topicName string, channelName string) (<-chan *Message, error)
	Unsubscribe(topicName string, channelName string) error

	// 事务支持
	BeginTx() (Transaction, error)
}

// Transaction 定义了事务接口
type Transaction interface {
	Commit() error
	Rollback() error
}

// Topic 主题实体
type Topic struct {
	Name         string    `json:"name"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
	MessageCount int64     `json:"message_count"`
	MessageBytes int64     `json:"message_bytes"`
	Paused       bool      `json:"paused"`
}

// Channel 通道实体
type Channel struct {
	ID           int64     `json:"id"`
	Topic        string    `json:"topic"`
	Name         string    `json:"name"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
	MessageCount int64     `json:"message_count"`
	Paused       bool      `json:"paused"`
}

// Message 消息实体
type Message struct {
	ID        string    `json:"id"`
	Topic     string    `json:"topic"`
	Body      []byte    `json:"body"`
	CreatedAt time.Time `json:"created_at"`
	Deferred  int64     `json:"deferred"`
}

// ChannelMessage 通道消息实体
type ChannelMessage struct {
	ID          int64     `json:"id"`
	ChannelID   int64     `json:"channel_id"`
	MessageID   string    `json:"message_id"`
	CreatedAt   time.Time `json:"created_at"`
	Delivered   bool      `json:"delivered"`
	DeliveredAt time.Time `json:"delivered_at,omitempty"`
	Finished    bool      `json:"finished"`
	FinishedAt  time.Time `json:"finished_at,omitempty"`
	Requeued    bool      `json:"requeued"`
	RequeuedAt  time.Time `json:"requeued_at,omitempty"`
	Attempts    int       `json:"attempts"`
}
