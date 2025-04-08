package nsqite

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Topic 表示一个消息主题
type Topic struct {
	Name         string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	MessageCount int64
	MessageBytes int64
	Paused       bool
	channels     map[string]*nsqChannel
	mu           sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
}

// Channel 表示一个消息通道
type Channel struct {
	ID               int64
	Name             string
	messages         chan *Message
	inFlightMessages map[string]*Message // 正在处理中的消息
	mu               sync.RWMutex
	ctx              context.Context
	cancel           context.CancelFunc
}

// NewTopic 创建一个新的Topic
func NewTopic(name string) *Topic {
	ctx, cancel := context.WithCancel(context.Background())
	return &Topic{
		Name:         name,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
		MessageCount: 0,
		MessageBytes: 0,
		Paused:       false,
		channels:     make(map[string]*nsqChannel),
		ctx:          ctx,
		cancel:       cancel,
	}
}

// Stop 停止Topic
func (t *Topic) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, ch := range t.channels {
		ch.Stop()
	}
	t.channels = make(map[string]*nsqChannel)
	slog.Debug("Topic已停止", "topic", t.Name)
}

// GetChannel 获取Channel
func (t *Topic) GetChannel(name string) *nsqChannel {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.channels[name]
}

// AddChannel 添加Channel
func (t *Topic) AddChannel(ch *nsqChannel) *nsqChannel {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 检查Channel是否已存在
	if existing, ok := t.channels[ch.Name]; ok {
		return existing
	}

	// 添加到channels
	t.channels[ch.Name] = ch

	return ch
}

// AddMessage 添加消息
func (t *Topic) AddMessage(msg *Message) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 更新消息计数
	t.MessageCount++
	t.MessageBytes += int64(len(msg.Body))
	t.UpdatedAt = time.Now()

	// 将消息发送到所有Channel
	for _, ch := range t.channels {
		ch.AddMessage(msg)
	}
}

// messagePump 消息泵，定期检查超时消息
func (t *Topic) messagePump() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			t.mu.RLock()
			for _, ch := range t.channels {
				ch.mu.RLock()
				// 检查超时消息
				now := time.Now()
				for msg := range ch.messages {
					if now.Sub(msg.CreatedAt) > 30*time.Second {
						// 重新入队
						select {
						case ch.messages <- msg:
						default:
							slog.Warn("重新入队失败，Channel消息队列已满", "topic", t.Name, "channel", ch.Name)
						}
					}
				}
				ch.mu.RUnlock()
			}
			t.mu.RUnlock()
		}
	}
}

// Stop 停止Channel
func (c *Channel) Stop() {
	c.cancel()
	close(c.messages)
}

// GetMessages 获取通道消息
func (c *Channel) GetMessages() <-chan *Message {
	return c.messages
}

// RequeueMessage 重新入队消息
func (c *Channel) RequeueMessage(msg *Message) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 从正在处理中的消息中移除
	delete(c.inFlightMessages, msg.ID)

	// 重新入队
	select {
	case c.messages <- msg:
		slog.Debug("消息重新入队成功", "messageID", msg.ID)
	default:
		slog.Warn("重新入队失败，Channel消息队列已满", "channel", c.Name)
	}
}
