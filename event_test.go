package nsqite

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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
	subscriber := NewSubscriber[string](topic, "test-subscriber", WithMaxAttempts(5))
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
	const maxAttempts = 5

	// 创建一个计数器来跟踪消息处理的尝试次数
	type attemptTracker struct {
		attempts int32
		finished bool
	}

	tracker := &attemptTracker{}

	// 创建发布者和订阅者
	publisher := NewPublisher[string]()
	subscriber := NewSubscriber[string](topic, "max-attempts-consumer", WithMaxAttempts(maxAttempts))

	// 创建一个总是返回错误的处理器，强制消息重试
	handler := EventHandler[string](SubscriberHandlerFunc[string](func(message *EventMessage[string]) error {
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

// 测试发布超时
func TestPublishTimeout(t *testing.T) {
	t.Parallel()
	const topic = "timeout-test"

	// 创建发布者和订阅者
	p := NewPublisher[string]()
	s := NewSubscriber[string](topic, "timeout-consumer", WithQueueSize(1))

	// 创建一个处理消息很慢的处理器
	cache := make(map[string]struct{})
	s.AddConcurrentHandlers(SubscriberHandlerFunc[string](func(message *EventMessage[string]) error {
		time.Sleep(1 * time.Second)
		cache[message.Body] = struct{}{}
		return nil
	}), 1)
	p.Publish(topic, "timeout-message")
	p.Publish(topic, "timeout-message")

	// 创建一个带有短超时的上下文
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// 发布消息，应该超时
	err := p.PublishWithContext(ctx, topic, "timeout-message")

	// 验证是否返回了超时错误
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("预期发布超时错误，但得到了: %v", err)
	}

	// 使用足够长的超时时间再次尝试
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()

	err = p.PublishWithContext(ctx2, topic, "success-message")
	if err != nil {
		t.Errorf("使用足够长的超时时间发布应该成功，但得到了错误: %v", err)
	}

	// 等待消息处理完成
	s.WaitMessage()

	// 验证成功消息是否被接收
	if _, ok := cache["success-message"]; !ok {
		t.Error("成功消息未被接收")
	}
}

// 测试发布者在订阅者队列满且返回错误时的行为
// 队列不会阻塞
func TestPublishWithQueueLimitAndError(t *testing.T) {
	t.Parallel()
	const topic = "queue-limit-test"

	// 创建发布者和两个订阅者，限制队列长度为2
	p := NewPublisher[string]()
	s1 := NewSubscriber[string](topic, "limit-consumer-1", WithQueueSize(2))
	s2 := NewSubscriber[string](topic, "limit-consumer-2", WithQueueSize(2))

	// 计数器
	var count1, count2 int32

	// 第一个订阅者：处理到第3条消息时返回错误
	s1.AddConcurrentHandlers(SubscriberHandlerFunc[string](func(message *EventMessage[string]) error {
		atomic.AddInt32(&count1, 1)
		return errors.New("故意返回错误")
	}), 1)

	// 第二个订阅者：处理到第3条消息时返回错误
	s2.AddConcurrentHandlers(SubscriberHandlerFunc[string](func(message *EventMessage[string]) error {
		atomic.AddInt32(&count2, 1)
		return errors.New("故意返回错误")
	}), 1)

	// 尝试发布10条消息
	var publishCount int
	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("message-%d", i)
		err := p.Publish(topic, msg)
		if err == nil {
			publishCount++
		}
	}

	// 等待消息处理完成
	s1.WaitMessage()
	s2.WaitMessage()

	// 验证结果
	t.Logf("成功发布消息数: %d", publishCount)
	t.Logf("订阅者1处理消息数: %d", atomic.LoadInt32(&count1))
	t.Logf("订阅者2处理消息数: %d", atomic.LoadInt32(&count2))

	// 由于队列限制为2，加上正在处理的1条，理论上每个订阅者最多能接收3条消息
	if publishCount != 10 {
		t.Errorf("预期最多发布 10 条消息，但实际发布了%d条", publishCount)
	}
}

// TestConcurrentPublishSubscribe2 测试 a 阻塞了，b 会阻塞吗?
// 结论，不会
func TestConcurrentPublishSubscribe2(t *testing.T) {
	// 创建两个通道用于等待两个协程完成
	done1 := make(chan struct{})
	done2 := make(chan struct{})

	// 第一个协程：发布者和订阅者受阻
	go func() {
		defer close(done1)

		const topic = "test-topic-blocked"

		// 创建发布者和两个订阅者
		p := NewPublisher[string]()
		s1 := NewSubscriber[string](topic, "consumer1", WithQueueSize(2))
		s2 := NewSubscriber[string](topic, "consumer2", WithQueueSize(2))

		// 第一个订阅者：处理到第3条消息时返回错误
		s1.AddConcurrentHandlers(SubscriberHandlerFunc[string](func(message *EventMessage[string]) error {
			time.Sleep(20 * time.Second)
			return errors.New("故意返回错误")
		}), 1)

		// 第二个订阅者：处理到第3条消息时返回错误
		s2.AddConcurrentHandlers(SubscriberHandlerFunc[string](func(message *EventMessage[string]) error {
			time.Sleep(20 * time.Second)
			return errors.New("故意返回错误")
		}), 1)

		// 尝试发布10条消息
		var publishCount int
		for i := 0; i < 10; i++ {
			msg := fmt.Sprintf("blocked-message-%d", i)
			err := p.Publish(topic, msg)
			if err == nil {
				publishCount++
			}
		}

		// 等待消息处理完成
		s1.WaitMessage()
		s2.WaitMessage()

		// 记录结果
		t.Logf("阻塞场景 - 成功发布消息数: %d", publishCount)
	}()

	// 第二个协程：正常发布和订阅
	go func() {
		time.Sleep(time.Second)
		defer close(done2)

		const topic = "test-topic-normal"

		// 创建发布者和订阅者
		p := NewPublisher[string]()
		s := NewSubscriber[string](topic, "normal-consumer")

		// 计数器
		var count int32
		var wg sync.WaitGroup
		wg.Add(100) // 期望处理100条消息

		// 正常订阅者
		s.AddConcurrentHandlers(SubscriberHandlerFunc[string](func(message *EventMessage[string]) error {
			atomic.AddInt32(&count, 1)
			wg.Done()
			return nil
		}), 1)

		// 发布100条消息
		for i := 0; i < 100; i++ {
			msg := fmt.Sprintf("normal-message-%d", i)
			err := p.Publish(topic, msg)
			if err != nil {
				t.Logf("正常场景 - 发布消息失败: %v", err)
			}
		}

		// 等待所有消息处理完成
		wg.Wait()
		s.WaitMessage()

		// 记录结果
		t.Logf("正常场景 - 订阅者处理消息数: %d", atomic.LoadInt32(&count))
	}()

	// 等待两个协程完成
	<-done2
	select {
	case <-done1:
	case <-time.After(time.Second * 5):
		fmt.Println("协程1按预料超时")
	}

	// 验证两个场景是否互相影响
	t.Log("两个协程都已完成，测试结束")
}

func TestEventMessageClose(t *testing.T) {
	var wg sync.WaitGroup
	for i := range 100000 {
		topic := "test-topic-close" + strconv.Itoa(i)
		p := NewPublisher[string]()
		s1 := NewSubscriber[string](topic, "limit-consumer-1", WithQueueSize(2))
		s1.AddConcurrentHandlers(SubscriberHandlerFunc[string](func(message *EventMessage[string]) error {
			return nil
		}), 1)

		wg.Add(2)
		go func() {
			defer wg.Done()
			for range 10 {
				p.Publish(topic, "msg")
			}
		}()
		go func() {
			defer wg.Done()
			s1.Stop()
		}()
	}
	time.Sleep(time.Second)
}
