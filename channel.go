package nsqite

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// nsqChannel 表示一个消息通道
type nsqChannel struct {
	ID               int64
	TopicID          string // 使用 Topic 的 Name 作为 ID
	Name             string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	MessageCount     int64
	Paused           bool
	messages         chan *Message
	inFlightMessages map[string]*Message
	mu               sync.RWMutex
	ctx              context.Context
	cancel           context.CancelFunc
}

// NewChannel 创建一个新的Channel
func NewChannel(topicName string, name string) *nsqChannel {
	ctx, cancel := context.WithCancel(context.Background())
	return &nsqChannel{
		ID:               time.Now().UnixNano(), // 使用时间戳作为ID
		TopicID:          topicName,             // 使用 Topic 的 Name 作为 ID
		Name:             name,
		CreatedAt:        time.Now(),
		UpdatedAt:        time.Now(),
		MessageCount:     0,
		Paused:           false,
		messages:         make(chan *Message, 1000),
		inFlightMessages: make(map[string]*Message),
		mu:               sync.RWMutex{},
		ctx:              ctx,
		cancel:           cancel,
	}
}

// AddMessage 添加消息到Channel
func (c *nsqChannel) AddMessage(msg *Message) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 更新消息计数
	c.MessageCount++
	c.UpdatedAt = time.Now()

	// 发送消息
	select {
	case c.messages <- msg:
		// 消息已发送
	default:
		slog.Warn("Channel消息队列已满", "channel", c.Name)
	}
}

// RequeueMessage 重新入队消息
func (c *nsqChannel) RequeueMessage(msg *Message) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 从飞行中消息中移除
	delete(c.inFlightMessages, msg.ID)

	// 重新入队
	select {
	case c.messages <- msg:
		// 消息已重新入队
	default:
		slog.Warn("Channel消息队列已满，无法重新入队", "channel", c.Name, "messageID", msg.ID)
	}
}

// Stop 停止Channel
func (c *nsqChannel) Stop() {
	c.cancel()
	close(c.messages)
	slog.Debug("Channel已停止", "channel", c.Name)
}
