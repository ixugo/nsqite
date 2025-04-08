package nsqite

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Consumer 表示一个消息消费者
type Consumer struct {
	nsqite      *NSQite
	topicName   string
	channelName string
	handler     MessageHandler
	mu          sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
}

// MessageHandler 定义消息处理函数类型
type MessageHandler func(msg *Message) error

// NewConsumer 创建一个新的Consumer
func NewConsumer(nsqite *NSQite, topicName, channelName string) (*Consumer, error) {
	ctx, cancel := context.WithCancel(context.Background())
	consumer := &Consumer{
		nsqite:      nsqite,
		topicName:   topicName,
		channelName: channelName,
		ctx:         ctx,
		cancel:      cancel,
	}

	// 确保Channel存在
	if _, err := nsqite.CreateChannel(topicName, channelName); err != nil {
		cancel()
		return nil, err
	}

	return consumer, nil
}

// SetHandler 设置消息处理函数
func (c *Consumer) SetHandler(handler MessageHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handler = handler
}

// Start 开始消费消息
func (c *Consumer) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.handler == nil {
		return ErrNoHandler
	}

	// 获取Channel
	channel, err := c.nsqite.GetChannel(c.topicName, c.channelName)
	if err != nil {
		return err
	}

	// 启动消息泵
	go c.messagePump(channel)

	return nil
}

// messagePump 消息泵，处理消息超时和重试
func (c *Consumer) messagePump(channel *nsqChannel) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			// 检查超时消息
			now := time.Now()
			channel.mu.RLock()
			for _, msg := range channel.inFlightMessages {
				if now.Sub(msg.StartTime) > msg.Timeout {
					// 消息超时，重新入队
					channel.RequeueMessage(msg)
				}
			}
			channel.mu.RUnlock()
		}
	}
}

// Stop 停止消费
func (c *Consumer) Stop() {
	c.cancel()
}

// Subscribe 订阅消息
func (c *Consumer) Subscribe() (<-chan *Message, error) {
	// 获取Channel
	channel, err := c.nsqite.GetChannel(c.topicName, c.channelName)
	if err != nil {
		return nil, err
	}

	// 创建消息通道
	msgChan := make(chan *Message, 100)

	// 启动消息处理协程
	go func() {
		defer close(msgChan)

		for {
			select {
			case <-c.ctx.Done():
				return
			case msg := <-channel.messages:
				// 处理消息
				if err := c.handler(msg); err != nil {
					slog.Error("消息处理失败", "error", err, "messageID", msg.ID)
					// 重新入队
					channel.RequeueMessage(msg)
					continue
				}

				// 发送到消息通道
				select {
				case <-c.ctx.Done():
					return
				case msgChan <- msg:
				}
			}
		}
	}()

	return msgChan, nil
}
