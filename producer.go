package nsqite

import (
	"context"
	"sync"

	"gorm.io/gorm"
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
// 此函数会返回 err，正常使用发布订阅不会出错，可以直接丢弃 err 不处理
func (p *Producer) Publish(topicName string, body []byte) error {
	// return p.PublishWithContext(context.Background(), topicName, body)
	return nil
}

func (p *Producer) PublishTx(tx *gorm.DB, topicName string, body []byte) error {
	// return p.PublishWithContext(tx.Context(), topicName, body)
	return nil
}

// Stop 停止Producer
func (p *Producer) Stop() {
	p.cancel()
}
