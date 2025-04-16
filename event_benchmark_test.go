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
	c.AddConcurrentHandlers(HandlerFunc[string](func(message *EventMessage[string]) error {
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
