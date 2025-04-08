package nsqite

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ixugo/nsqite/storage"
)

// Producer 表示一个消息生产者
type Producer struct {
	nsqite *NSQite
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
}

// NewProducer 创建一个新的Producer
func NewProducer(nsqite *NSQite) *Producer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Producer{
		nsqite: nsqite,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Publish 发布消息到Topic
func (p *Producer) Publish(topicName string, body []byte) error {
	return p.PublishWithContext(context.Background(), topicName, body)
}

// PublishWithContext 带上下文的发布消息
func (p *Producer) PublishWithContext(ctx context.Context, topicName string, body []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 获取或创建Topic（懒加载）
	topic, err := p.nsqite.GetTopic(topicName)
	if err != nil {
		return err
	}

	// 创建消息
	msgID := uuid.New().String()
	message := &storage.Message{
		ID:        msgID,
		Topic:     topic.Name,
		Body:      body,
		CreatedAt: time.Now(),
	}

	// 保存消息
	if err := p.nsqite.store.SaveMessage(message); err != nil {
		return err
	}

	// 创建NSQite消息
	nsqMsg := &Message{
		ID:        msgID,
		Body:      body,
		CreatedAt: time.Now(),
		Delegate:  &MDelegate{},
	}

	// 添加到Topic
	topic.AddMessage(nsqMsg)

	// 为每个Channel创建ChannelMessage
	channels, err := p.nsqite.store.FindChannels(topicName)
	if err != nil {
		return err
	}

	for _, ch := range channels {
		channelMessage := &storage.ChannelMessage{
			ChannelID: ch.ID,
			MessageID: msgID,
			CreatedAt: time.Now(),
		}

		if err := p.nsqite.store.SaveChannelMessage(channelMessage); err != nil {
			return err
		}
	}

	slog.Debug("消息发布成功", "topic", topicName, "messageID", msgID)
	return nil
}

// Stop 停止Producer
func (p *Producer) Stop() {
	p.cancel()
}
