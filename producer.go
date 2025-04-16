package nsqite

import (
	"time"

	"gorm.io/gorm"
)

// Producer 表示一个消息生产者
type Producer struct{}

// NewProducer 创建一个新的Producer
func NewProducer() *Producer {
	return &Producer{}
}

// Publish 发布消息
// 此函数会返回 err，正常使用发布订阅不会出错，可以直接丢弃 err 不处理
func (p *Producer) Publish(topic string, body []byte) error {
	return TransactionMQ().Publish(topic, &Message{
		Body:      body,
		Timestamp: time.Now(),
		Topic:     topic,
	})
}

// PublishTx 发布事务消息
func (p *Producer) PublishTx(tx *gorm.DB, topic string, body []byte) error {
	return TransactionMQ().PublishTx(tx, topic, &Message{
		Body:      body,
		Timestamp: time.Now(),
		Topic:     topic,
	})
}
