package nsqite

import (
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func cleanUp() {
	os.Remove("test.db")
}

func initDB() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: true,
	})))

	// :memory:?_journal=WAL&_timeout=5000&_fk=true
	gormDB, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	db, err := gormDB.DB()
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	SetGorm(gormDB)
}

// TestNSQite 测试消费消息
func TestNSQite(t *testing.T) {
	cleanUp()
	initDB()

	const topic = "test"
	const messageBody = "hello world"

	// 2. 创建生产者和消费者
	p := NewProducer()
	c := NewConsumer(topic, "test-channel")

	// 3. 设置消息处理函数
	done := make(chan bool)
	c.AddConcurrentHandlers(func(msg *Message) error {
		if string(msg.Body) != messageBody {
			t.Errorf("expected message body %s, got %s", messageBody, string(msg.Body))
		}
		done <- true
		return nil
	}, 1)

	// 4. 发布消息
	if err := p.Publish(topic, []byte(messageBody)); err != nil {
		t.Fatal(err)
	}

	// 5. 等待消息处理完成
	select {
	case <-done:
		// 测试通过
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message processing")
	}

	time.Sleep(time.Second)
}

// TestMaxAttempts 测试最大重试次数
func TestMaxAttempts(t *testing.T) {
	cleanUp()
	initDB()
	const topic = "test-max-attempts"
	const messageBody = "test message"

	p := NewProducer()
	c := NewConsumer(topic, "test-channel", WithConsumerMaxAttempts(3))

	attempts := uint32(0)
	c.AddConcurrentHandlers(func(msg *Message) error {
		msg.DisableAutoResponse()

		atomic.AddUint32(&attempts, 1)

		msg.Requeue(0)

		return fmt.Errorf("simulated error")
	}, 1)

	if err := p.Publish(topic, []byte(messageBody)); err != nil {
		t.Fatal(err)
	}

	c.WaitMessage()
	time.Sleep(time.Second)

	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}
