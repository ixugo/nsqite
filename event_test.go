package nsqite

import (
	"fmt"
	"testing"
	"time"
)

// 适用于单体
// 模拟作者 a 写书，写好了要发给一家出版社
// 模拟作者 b 写书，写好了要分别发给两家出版社
var (
	_ EventHandler[string] = (*Reader1)(nil)
	_ EventHandler[string] = (*Reader2)(nil)
)

type Reader1 struct{}

// HandleMessage implements Handler.
func (r *Reader1) HandleMessage(message *EventMessage[string]) error {
	time.Sleep(time.Second)
	fmt.Println("reader one :", message.Body)
	return nil
}

type Reader2 struct{}

// HandleMessage implements Handler.
func (r *Reader2) HandleMessage(message *EventMessage[string]) error {
	fmt.Println("reader two :", message.Body)
	return nil
}

func TestSubscriber(t *testing.T) {
	t.Parallel()
	t.Run("a", func(t *testing.T) {
		const topic = "a-book"
		p := NewPublisher[string]()
		c := NewSubscriber[string](topic, "comsumer1")
		c.AddConcurrentHandlers(&Reader1{}, 1)

		p.Publish(topic, "a >> hello")
	})

	t.Run("b", func(t *testing.T) {
		const topic = "b-book"
		p := NewPublisher[string]()
		c := NewSubscriber[string](topic, "comsumer1")
		c.AddConcurrentHandlers(&Reader1{}, 1)
		c2 := NewSubscriber[string](topic, "comsumer2")
		c2.AddConcurrentHandlers(&Reader2{}, 1)

		p.Publish(topic, "b >> hello")
	})
	time.Sleep(time.Second * 2)
}

// 模拟一个作者疯狂写书，出版社派出 5 个编辑，每个编辑每秒只能处理一本书
func TestPublisher(t *testing.T) {
	const topic = "a-book"
	p := NewPublisher[string]()
	c := NewSubscriber[string](topic, "comsumer1")
	c.AddConcurrentHandlers(&Reader1{}, 5)

	for i := 0; i < 5; i++ {
		p.Publish(topic, fmt.Sprintf("a >> hello %d", i))
	}

	time.Sleep(2 * time.Second)
}
