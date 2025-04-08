package nsqite

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ixugo/nsqite/storage"
)

// NSQite 是消息队列的核心结构
type NSQite struct {
	store  storage.Storage
	topics map[string]*Topic
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
}

// New 创建一个新的NSQite实例
func New(store storage.Storage) (*NSQite, error) {
	ctx, cancel := context.WithCancel(context.Background())

	nsqite := &NSQite{
		store:  store,
		topics: make(map[string]*Topic),
		ctx:    ctx,
		cancel: cancel,
	}

	// 初始化存储
	if err := store.Init(nil); err != nil {
		return nil, fmt.Errorf("初始化存储失败: %w", err)
	}

	// 启动消息泵
	go nsqite.messagePump()

	return nsqite, nil
}

// Close 关闭NSQite实例
func (n *NSQite) Close() error {
	n.cancel()

	// 关闭所有topics
	n.mu.Lock()
	for _, t := range n.topics {
		t.Stop()
	}
	n.mu.Unlock()

	// 关闭存储
	return n.store.Close()
}

// GetTopic 获取或创建Topic（懒加载）
func (n *NSQite) GetTopic(name string) (*Topic, error) {
	n.mu.RLock()
	t, ok := n.topics[name]
	n.mu.RUnlock()

	if ok {
		return t, nil
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// 双重检查
	if t, ok = n.topics[name]; ok {
		return t, nil
	}

	// 创建新Topic（仅在内存中）
	t = NewTopic(name)
	n.topics[name] = t
	slog.Debug("创建新Topic", "topic", name)

	return t, nil
}

// CreateChannel 创建一个新的Channel
func (n *NSQite) CreateChannel(topicName, channelName string) (*nsqChannel, error) {
	// 获取或创建Topic（懒加载）
	t, err := n.GetTopic(topicName)
	if err != nil {
		return nil, err
	}

	// 检查Channel是否已存在
	if ch := t.GetChannel(channelName); ch != nil {
		return ch, nil
	}

	// 创建Channel
	ch := NewChannel(topicName, channelName)

	// 添加到Topic
	t.AddChannel(ch)

	// 保存到存储
	channelModel := &storage.Channel{
		Topic:     topicName, // 使用 Topic 的 Name 作为 ID
		Name:      channelName,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if err := n.store.AddChannel(channelModel); err != nil {
		return nil, fmt.Errorf("保存Channel失败: %w", err)
	}

	return ch, nil
}

// GetChannel 获取Channel
func (n *NSQite) GetChannel(topicName, channelName string) (*nsqChannel, error) {
	topic, err := n.GetTopic(topicName)
	if err != nil {
		return nil, err
	}

	channel := topic.GetChannel(channelName)
	if channel == nil {
		return nil, fmt.Errorf("Channel不存在: %s/%s", topicName, channelName)
	}

	return channel, nil
}

// Publish 发布消息到Topic
func (n *NSQite) Publish(topicName string, body []byte) error {
	return n.PublishWithContext(context.Background(), topicName, body)
}

func (n *NSQite) PublishTx(ctx context.Context, topic string, body []byte) error {
	// return n.store.Tx(ctx, func(ctx context.Context) error {
	// return n.PublishWithContext(ctx, topicName, body)
	// })
	return nil
}

// PublishWithContext 带上下文的发布消息
func (n *NSQite) PublishWithContext(ctx context.Context, topic string, body []byte) error {
	// 获取或创建Topic（懒加载）
	t, err := n.GetTopic(topic)
	if err != nil {
		return err
	}

	// 创建消息
	msgID := uuid.New().String()
	message := &storage.Message{
		ID:        msgID,
		Topic:     t.Name,
		Body:      body,
		CreatedAt: time.Now(),
	}

	// 保存消息
	if err := n.store.SaveMessage(message); err != nil {
		return fmt.Errorf("保存消息失败: %w", err)
	}

	// 创建NSQite消息
	nsqMsg := &Message{
		ID:        msgID,
		Body:      body,
		CreatedAt: time.Now(),
		Delegate:  &MDelegate{},
	}

	// 添加到Topic
	t.AddMessage(nsqMsg)

	// 为每个Channel创建ChannelMessage
	channels, err := n.store.FindChannels(topic)
	if err != nil {
		return fmt.Errorf("获取Channels失败: %w", err)
	}

	for _, ch := range channels {
		channelMessage := &storage.ChannelMessage{
			ChannelID: ch.ID,
			MessageID: msgID,
			CreatedAt: time.Now(),
		}

		if err := n.store.SaveChannelMessage(channelMessage); err != nil {
			return fmt.Errorf("保存ChannelMessage失败: %w", err)
		}
	}

	return nil
}

// Subscribe 订阅Topic的Channel
func (n *NSQite) Subscribe(topicName, channelName string) (<-chan *Message, error) {
	// 获取或创建Channel
	_, err := n.CreateChannel(topicName, channelName)
	if err != nil {
		return nil, err
	}

	// 从存储订阅消息
	storageMsgChan, err := n.store.Subscribe(topicName, channelName)
	if err != nil {
		return nil, err
	}

	// 转换为NSQite消息
	msgChan := make(chan *Message, 100)
	go func() {
		defer close(msgChan)
		for msg := range storageMsgChan {
			nsqMsg := &Message{
				ID:        msg.ID,
				Body:      msg.Body,
				CreatedAt: msg.CreatedAt,
				Delegate:  &MDelegate{},
			}
			msgChan <- nsqMsg
		}
	}()

	return msgChan, nil
}

// messagePump 消息泵，处理消息超时和重试
func (n *NSQite) messagePump() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			// 检查所有Topic的Channel
			n.mu.RLock()
			for _, topic := range n.topics {
				topic.mu.RLock()
				for _, channel := range topic.channels {
					// 获取未完成的消息
					channelMessages, err := n.store.FindChannelMessages(
						int(channel.ID),
						1, // delivered
						0, // finished
						0, // requeued
						100,
						0,
					)
					if err != nil {
						slog.Error("获取未完成消息失败", "error", err, "channel", channel.Name)
						continue
					}

					now := time.Now()
					for _, cm := range channelMessages {
						// 如果消息超过30秒未完成，重新入队
						if now.Sub(cm.CreatedAt) > 30*time.Second {
							cm.Requeued = true
							cm.RequeuedAt = now

							if err := n.store.EditChannelMessage(cm); err != nil {
								slog.Error("更新超时消息失败", "error", err, "messageID", cm.MessageID)
								continue
							}

							// 获取消息
							message, err := n.store.GetMessage(cm.MessageID)
							if err != nil {
								slog.Error("获取消息失败", "error", err, "messageID", cm.MessageID)
								continue
							}

							// 创建NSQite消息
							nsqMsg := &Message{
								ID:        message.ID,
								Body:      message.Body,
								CreatedAt: message.CreatedAt,
								Delegate:  &MDelegate{},
								Timeout:   30 * time.Second,
							}

							// 重新入队
							channel.RequeueMessage(nsqMsg)
						}
					}
				}
				topic.mu.RUnlock()
			}
			n.mu.RUnlock()
		}
	}
}

// RequeueMessage 重新入队消息
func (n *NSQite) RequeueMessage(topicName, channelName, messageID string, timeout time.Duration) error {
	// 获取Channel
	channel, err := n.GetChannel(topicName, channelName)
	if err != nil {
		return err
	}

	// 获取消息
	msg, err := n.store.GetMessage(messageID)
	if err != nil {
		return err
	}

	// 创建NSQite消息
	nsqMsg := &Message{
		ID:        msg.ID,
		Body:      msg.Body,
		CreatedAt: msg.CreatedAt,
		Delegate:  &MDelegate{},
		Timeout:   timeout,
	}

	// 重新入队消息
	channel.RequeueMessage(nsqMsg)

	// 更新ChannelMessage状态
	cm, err := n.store.GetChannelMessage(int(channel.ID), messageID)
	if err != nil {
		return err
	}

	cm.Requeued = true
	cm.RequeuedAt = time.Now()

	if err := n.store.EditChannelMessage(cm); err != nil {
		return err
	}

	return nil
}
