package nsqite

import (
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkPublish(b *testing.B) {
	const topic = "benchmark-topic"
	p := NewPublisher[string]()
	c := NewSubscriber[string](topic, "benchmark-consumer")

	done := make(chan struct{})
	var receivedCount int64
	c.AddConcurrentHandlers(SubscriberHandlerFunc[string](func(message *EventMessage[string]) error {
		if atomic.AddInt64(&receivedCount, 1) == int64(b.N) {
			close(done)
		}
		return nil
	}), 1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Publish(topic, "test-message")
	}
	c.WaitMessage()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}

	b.StopTimer() // 停止计时器

	// 输出统计信息
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/sec")
}

func BenchmarkPublish3(b *testing.B) {
	done := make(chan struct{})
	done2 := make(chan struct{})
	done3 := make(chan struct{})
	var receivedCount, receivedCount2, receivedCount3 int64
	const topic = "benchmark-topic"

	p := NewPublisher[string]()
	c := NewSubscriber[string](topic, "benchmark-consumer")
	c.AddConcurrentHandlers(SubscriberHandlerFunc[string](func(message *EventMessage[string]) error {
		if atomic.AddInt64(&receivedCount, 1) == int64(b.N) {
			close(done)
		}
		return nil
	}), 1)

	c2 := NewSubscriber[string](topic, "benchmark-consumer2")
	c2.AddConcurrentHandlers(SubscriberHandlerFunc[string](func(message *EventMessage[string]) error {
		if atomic.AddInt64(&receivedCount2, 1) == int64(b.N) {
			close(done2)
		}
		return nil
	}), 1)

	c3 := NewSubscriber[string](topic, "benchmark-consumer3")
	c3.AddConcurrentHandlers(SubscriberHandlerFunc[string](func(message *EventMessage[string]) error {
		if atomic.AddInt64(&receivedCount3, 1) == int64(b.N) {
			close(done3)
		}
		return nil
	}), 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Publish(topic, "test-message")
	}
	c.WaitMessage()
	c2.WaitMessage()
	c3.WaitMessage()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}
	select {
	case <-done2:
	case <-time.After(5 * time.Second):
	}
	select {
	case <-done3:
	case <-time.After(5 * time.Second):
	}

	b.StopTimer() // 停止计时器

	// 输出统计信息
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/sec")
}
