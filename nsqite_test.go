package nsqite

import (
	"context"
	"testing"
	"time"

	"github.com/ixugo/nsqite/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNSQite(t *testing.T) {
	// 创建内存存储
	store := memory.NewMemoryStorage()
	require.NoError(t, store.Init(nil))

	// 创建NSQite实例
	nsqite, err := New(store)
	require.NoError(t, err)
	defer nsqite.Close()

	// 测试创建Topic
	topic, err := nsqite.GetTopic("test-topic")
	require.NoError(t, err)
	assert.Equal(t, "test-topic", topic.Name)

	// 测试创建Channel
	channel, err := nsqite.CreateChannel("test-topic", "test-channel")
	require.NoError(t, err)
	assert.Equal(t, "test-channel", channel.Name)

	// 测试发布消息
	err = nsqite.Publish("test-topic", []byte("test message"))
	require.NoError(t, err)

	// 测试订阅消息
	msgChan, err := nsqite.Subscribe("test-topic", "test-channel")
	require.NoError(t, err)

	// 接收消息
	select {
	case msg := <-msgChan:
		assert.Equal(t, []byte("test message"), msg.Body)
	case <-time.After(time.Second):
		t.Fatal("接收消息超时")
	}
}

func TestNSQiteWithContext(t *testing.T) {
	// 创建内存存储
	store := memory.NewMemoryStorage()
	require.NoError(t, store.Init(nil))

	// 创建NSQite实例
	nsqite, err := New(store)
	require.NoError(t, err)
	defer nsqite.Close()

	// 创建上下文
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// 测试带上下文的发布
	err = nsqite.PublishWithContext(ctx, "test-topic", []byte("test message"))
	require.NoError(t, err)

	// 测试带上下文的订阅
	msgChan, err := nsqite.Subscribe("test-topic", "test-channel")
	require.NoError(t, err)

	// 接收消息
	select {
	case msg := <-msgChan:
		assert.Equal(t, []byte("test message"), msg.Body)
	case <-time.After(time.Second):
		t.Fatal("接收消息超时")
	}
}

func TestNSQiteMessageTimeout(t *testing.T) {
	// 创建内存存储
	store := memory.NewMemoryStorage()
	require.NoError(t, store.Init(nil))

	// 创建NSQite实例
	nsqite, err := New(store)
	require.NoError(t, err)
	defer nsqite.Close()

	// 创建Topic和Channel
	_, err = nsqite.CreateChannel("test-topic", "test-channel")
	require.NoError(t, err)

	// 发布消息
	err = nsqite.Publish("test-topic", []byte("test message"))
	require.NoError(t, err)

	// 订阅消息
	msgChan, err := nsqite.Subscribe("test-topic", "test-channel")
	require.NoError(t, err)

	// 接收消息
	var msg *Message
	select {
	case msg = <-msgChan:
		assert.Equal(t, []byte("test message"), msg.Body)
	case <-time.After(time.Second):
		t.Fatal("接收消息超时")
	}

	// 等待消息超时
	time.Sleep(2 * time.Second)

	// 消息应该被重新入队
	select {
	case msg = <-msgChan:
		assert.Equal(t, []byte("test message"), msg.Body)
	case <-time.After(time.Second):
		t.Fatal("接收重试消息超时")
	}
}
