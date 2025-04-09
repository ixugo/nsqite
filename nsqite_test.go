package nsqite

import (
	"context"
	"testing"
	"time"

	"github.com/ixugo/nsqite/storage/memory"
)

func TestNSQite(t *testing.T) {
	// 创建内存存储
	store := memory.NewMemoryStorage()
	if err := store.Init(nil); err != nil {
		t.Fatalf("初始化存储失败: %v", err)
	}

	// 创建NSQite实例
	nsqite, err := New(store)
	if err != nil {
		t.Fatalf("创建NSQite实例失败: %v", err)
	}
	defer nsqite.Close()

	// 测试创建Topic
	topic, err := nsqite.GetTopic("test-topic")
	if err != nil {
		t.Fatalf("创建Topic失败: %v", err)
	}
	if topic.Name != "test-topic" {
		t.Errorf("期望topic名称为 'test-topic'，实际为 '%s'", topic.Name)
	}

	// 测试创建Channel
	channel, err := nsqite.CreateChannel("test-topic", "test-channel")
	if err != nil {
		t.Fatalf("创建Channel失败: %v", err)
	}
	if channel.Name != "test-channel" {
		t.Errorf("期望channel名称为 'test-channel'，实际为 '%s'", channel.Name)
	}

	// 测试发布消息
	if err := nsqite.Publish("test-topic", []byte("test message")); err != nil {
		t.Fatalf("发布消息失败: %v", err)
	}

	// 测试订阅消息
	msgChan, err := nsqite.Subscribe("test-topic", "test-channel")
	if err != nil {
		t.Fatalf("订阅消息失败: %v", err)
	}

	// 接收消息
	select {
	case msg := <-msgChan:
		if string(msg.Body) != "test message" {
			t.Errorf("期望消息内容为 'test message'，实际为 '%s'", string(msg.Body))
		}
	case <-time.After(time.Second):
		t.Fatal("接收消息超时")
	}
}

func TestNSQiteWithContext(t *testing.T) {
	// 创建内存存储
	store := memory.NewMemoryStorage()
	if err := store.Init(nil); err != nil {
		t.Fatalf("初始化存储失败: %v", err)
	}

	// 创建NSQite实例
	nsqite, err := New(store)
	if err != nil {
		t.Fatalf("创建NSQite实例失败: %v", err)
	}
	defer nsqite.Close()

	// 创建上下文
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// 测试带上下文的发布
	if err := nsqite.PublishWithContext(ctx, "test-topic", []byte("test message")); err != nil {
		t.Fatalf("发布消息失败: %v", err)
	}

	// 测试带上下文的订阅
	msgChan, err := nsqite.Subscribe("test-topic", "test-channel")
	if err != nil {
		t.Fatalf("订阅消息失败: %v", err)
	}

	// 接收消息
	select {
	case msg := <-msgChan:
		if string(msg.Body) != "test message" {
			t.Errorf("期望消息内容为 'test message'，实际为 '%s'", string(msg.Body))
		}
	case <-time.After(time.Second):
		t.Fatal("接收消息超时")
	}
}

func TestNSQiteMessageTimeout(t *testing.T) {
	// 创建内存存储
	store := memory.NewMemoryStorage()
	if err := store.Init(nil); err != nil {
		t.Fatalf("初始化存储失败: %v", err)
	}

	// 创建NSQite实例
	nsqite, err := New(store)
	if err != nil {
		t.Fatalf("创建NSQite实例失败: %v", err)
	}
	defer nsqite.Close()

	// 创建Topic和Channel
	_, err = nsqite.CreateChannel("test-topic", "test-channel")
	if err != nil {
		t.Fatalf("创建Channel失败: %v", err)
	}

	// 发布消息
	if err := nsqite.Publish("test-topic", []byte("test message")); err != nil {
		t.Fatalf("发布消息失败: %v", err)
	}

	// 订阅消息
	msgChan, err := nsqite.Subscribe("test-topic", "test-channel")
	if err != nil {
		t.Fatalf("订阅消息失败: %v", err)
	}

	// 接收消息
	var msg *Message
	select {
	case msg = <-msgChan:
		if string(msg.Body) != "test message" {
			t.Errorf("期望消息内容为 'test message'，实际为 '%s'", string(msg.Body))
		}
	case <-time.After(time.Second):
		t.Fatal("接收消息超时")
	}

	// 等待消息超时
	time.Sleep(2 * time.Second)

	// 消息应该被重新入队
	select {
	case msg = <-msgChan:
		if string(msg.Body) != "test message" {
			t.Errorf("期望消息内容为 'test message'，实际为 '%s'", string(msg.Body))
		}
	case <-time.After(time.Second):
		t.Fatal("接收重试消息超时")
	}
}
