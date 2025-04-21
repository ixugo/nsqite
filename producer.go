package nsqite

import (
	"time"
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

type SessionFunc func(*Message) error

// PublishTx publish database transaction messages
// createFn insert into function
// gorm example:
// PublishTx(func(v *nsqite.Message) error {
// 	return db.Create(v).Error
// }, "topic", []byte("body"))

func (p *Producer) PublishTx(createFn SessionFunc, topic string, body []byte) error {
	return TransactionMQ().PublishTx(createFn, topic, &Message{
		Body:      body,
		Timestamp: time.Now(),
		Topic:     topic,
	})
}
