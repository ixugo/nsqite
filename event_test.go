package nsqite

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

type Reader1 struct {
	receivedMessages sync.Map
}

// HandleMessage implements Handler.
func (r *Reader1) HandleMessage(message *EventMessage[string]) error {
	time.Sleep(10 * time.Millisecond)
	r.receivedMessages.Store(message.Body, true)
	return nil
}

type Reader2 struct {
	receivedMessages sync.Map
}

// HandleMessage implements Handler.
func (r *Reader2) HandleMessage(message *EventMessage[string]) error {
	r.receivedMessages.Store(message.Body, true)
	return nil
}

func TestSubscriber(t *testing.T) {
	t.Parallel()
	t.Run("a", func(t *testing.T) {
		const topic = "a-book"
		p := NewPublisher[string]()
		c := NewSubscriber[string](topic, "comsumer1")
		reader1 := &Reader1{}
		c.AddConcurrentHandlers(reader1, 1)

		expectedMsg := "a >> hello"
		p.Publish(topic, expectedMsg)
		time.Sleep(time.Second * 2)

		// 验证消息是否被接收
		if _, ok := reader1.receivedMessages.Load(expectedMsg); !ok {
			t.Errorf("Expected message %s was not received", expectedMsg)
		}
	})

	t.Run("b", func(t *testing.T) {
		const topic = "b-book"
		p := NewPublisher[string]()
		c := NewSubscriber[string](topic, "comsumer1")
		reader1 := &Reader1{}
		c.AddConcurrentHandlers(reader1, 1)
		c2 := NewSubscriber[string](topic, "comsumer2")
		reader2 := &Reader2{}
		c2.AddConcurrentHandlers(reader2, 1)

		expectedMsg := "b >> hello"
		p.Publish(topic, expectedMsg)
		time.Sleep(time.Second * 2)

		// 验证两个消费者都收到了消息
		if _, ok := reader1.receivedMessages.Load(expectedMsg); !ok {
			t.Errorf("Expected message %s was not received by reader1", expectedMsg)
		}
		if _, ok := reader2.receivedMessages.Load(expectedMsg); !ok {
			t.Errorf("Expected message %s was not received by reader2", expectedMsg)
		}
	})
}

// 模拟一个作者疯狂写书，出版社派出 5 个编辑，每个编辑每秒只能处理一本书
func TestPublisher(t *testing.T) {
	const topic = "a-book"
	p := NewPublisher[string]()
	c := NewSubscriber[string](topic, "comsumer1")
	reader1 := &Reader1{}
	c.AddConcurrentHandlers(reader1, 5)

	expectedMessages := make([]string, 5)
	for i := 0; i < 5; i++ {
		msg := fmt.Sprintf("a >> hello %d", i)
		expectedMessages[i] = msg
		p.Publish(topic, msg)
	}

	time.Sleep(2 * time.Second)

	// 验证所有消息是否都被接收
	for _, msg := range expectedMessages {
		if _, ok := reader1.receivedMessages.Load(msg); !ok {
			t.Errorf("Expected message %s was not received", msg)
		}
	}
}

type Reader3 struct {
	receivedMessages sync.Map
	attemptCount     int32
}

// HandleMessage implements Handler.
func (r *Reader3) HandleMessage(message *EventMessage[string]) error {
	atomic.AddInt32(&r.attemptCount, 1)
	// 禁用自动完成
	message.DisableAutoResponse()
	if message.Body == "hello" || message.Attempts > 3 {
		// 手动完成
		r.receivedMessages.Store(message.Body, true)
		message.Finish()
		return nil
	}
	// 手动延迟 1 秒后重试
	message.Requeue(300 * time.Millisecond)
	return nil
}

func TestDisableAutoResponse(t *testing.T) {
	topic := "test-topic"
	publisher := NewPublisher[string]()
	subscriber := NewSubscriber(topic, "test-subscriber", WithMaxAttempts[string](5))
	reader3 := &Reader3{}
	subscriber.AddConcurrentHandlers(reader3, 1)

	// 测试需要重试的消息
	publisher.Publish(topic, "test message")

	subscriber.WaitMessage()

	// 验证重试次数
	if atomic.LoadInt32(&reader3.attemptCount) != 4 {
		t.Error("Expected 4 attempts, got ", atomic.LoadInt32(&reader3.attemptCount))
	}
}

func TestMaxAttemptsLimit(t *testing.T) {
	topic := "max-attempts-test"
	const maxAttempts = 2

	// 创建一个计数器来跟踪消息处理的尝试次数
	type attemptTracker struct {
		attempts int32
		finished bool
	}

	tracker := &attemptTracker{}

	// 创建发布者和订阅者
	publisher := NewPublisher[string]()
	subscriber := NewSubscriber(topic, "max-attempts-consumer", WithMaxAttempts[string](maxAttempts))

	// 创建一个总是返回错误的处理器，强制消息重试
	handler := EventHandler[string](HandlerFunc[string](func(message *EventMessage[string]) error {
		atomic.AddInt32(&tracker.attempts, 1)
		return errors.New("强制失败以触发重试")
	}))

	subscriber.AddConcurrentHandlers(handler, 1)

	// 发布一条消息
	publisher.Publish(topic, "test-max-attempts")

	// 等待足够的时间让重试发生
	subscriber.WaitMessage()

	// 验证尝试次数不超过最大尝试次数
	attempts := atomic.LoadInt32(&tracker.attempts)
	if attempts > maxAttempts {
		t.Errorf("消息尝试次数超过了最大限制: 期望最多 %d 次，实际 %d 次", maxAttempts, attempts)
	}

	// 验证尝试次数应该正好等于最大尝试次数
	if attempts != maxAttempts {
		t.Errorf("消息尝试次数不符合预期: 期望 %d 次，实际 %d 次", maxAttempts, attempts)
	}
}

// HandlerFunc 是一个适配器，允许使用普通函数作为 EventHandler
type HandlerFunc[T any] func(message *EventMessage[T]) error

// HandleMessage 调用 f(m)
func (f HandlerFunc[T]) HandleMessage(m *EventMessage[T]) error {
	return f(m)
}

// 测试并发发布和订阅
func TestConcurrentPublishSubscribe(t *testing.T) {
	t.Parallel()
	const topic = "concurrent-test"
	const messageCount = 1000
	const consumerCount = 5

	// 创建发布者
	publisher := NewPublisher[string]()

	// 创建多个消费者
	consumers := make([]*Subscriber[string], consumerCount)
	readers := make([]*Reader1, consumerCount)
	for i := 0; i < consumerCount; i++ {
		readers[i] = &Reader1{}
		consumers[i] = NewSubscriber[string](topic, fmt.Sprintf("consumer-%d", i))
		consumers[i].AddConcurrentHandlers(readers[i], 5)
	}

	// 并发发布消息
	for i := range messageCount {
		go func(idx int) {
			publisher.Publish(topic, fmt.Sprintf("message-%d", idx))
		}(i)
	}

	for _, consumer := range consumers {
		consumer.WaitMessage()
	}

	// 验证每个消费者是否都收到了所有消息
	for i := 0; i < messageCount; i++ {
		expectedMsg := fmt.Sprintf("message-%d", i)
		for j := 0; j < consumerCount; j++ {
			if _, ok := readers[j].receivedMessages.Load(expectedMsg); !ok {
				t.Errorf("Consumer %d did not receive message %s", j, expectedMsg)
			}
		}
	}
}

// ErrorHandler 实现错误测试的处理器
type ErrorHandler struct {
	errorHandled bool
	mu           sync.Mutex
}

func (h *ErrorHandler) HandleMessage(message *EventMessage[string]) error {
	h.mu.Lock()
	h.errorHandled = true
	h.mu.Unlock()
	return fmt.Errorf("test error")
}

// 测试资源清理
func TestResourceCleanup(t *testing.T) {
	t.Parallel()
	const topic = "cleanup-test"

	// 创建发布者和消费者
	publisher := NewPublisher[string]()
	subscriber := NewSubscriber[string](topic, "cleanup-consumer")
	reader1 := &Reader1{}
	subscriber.AddConcurrentHandlers(reader1, 1)

	// 发布消息
	publisher.Publish(topic, "cleanup-message")
	time.Sleep(1 * time.Second)

	// 验证消息是否被接收
	if _, ok := reader1.receivedMessages.Load("cleanup-message"); !ok {
		t.Error("Expected message was not received before cleanup")
	}

	// 停止订阅者
	subscriber.Stop()

	// 等待资源清理
	time.Sleep(1 * time.Second)

	// 尝试发布新消息（应该不会收到）
	publisher.Publish(topic, "after-cleanup-message")
	time.Sleep(1 * time.Second)

	// 验证新消息是否未被接收
	if _, ok := reader1.receivedMessages.Load("after-cleanup-message"); ok {
		t.Error("Message was received after cleanup, which should not happen")
	}
}
