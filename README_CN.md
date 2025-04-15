
<p align="center">
    <img src="logo.webp" alt="NSQite Logo" width="550"/>
</p>

[English](./README.md) [中文](./README.md)

Go 语言实现的轻量级消息队列，支持 SQLite、PostgreSQL 和 ORM 作为持久化存储。

## 介绍

在项目初期，你可能不需要像 NSQ、Pulsar 这样的大型消息队列系统。NSQite 提供了一个简单可靠的解决方案，满足基本的消息队列需求。

NSQite 支持多种存储方式：
- SQLite 作为消息队列持久化
- PostgreSQL 作为消息队列持久化

NSQite 的 API 设计与 go-nsq 类似，方便未来项目升级到 NSQ 以支持更高并发。

注意：NSQite 保证消息至少被传递一次，可能会出现重复消息。消费者需要实现去重或幂等操作。

![](./docs/1.gif)

## 快速开始

### 事件总线

适用于单体架构中的业务场景，基于内存实现，支持发布者与订阅者 1:N 关系，包含任务失败延迟重试机制。

适用场景：
+ 单体架构
+ 需要实时通知订阅者
+ 支持服务重启后的消息补偿
+ 支持泛型消息体

示例场景：
**系统发生告警时，需要同时记录到数据库并通过 WebSocket 通知客户端**

1. 数据库记录模块订阅告警主题
2. WebSocket 通知模块订阅告警主题
3. 告警发生时，发布者发送告警消息
4. 两个订阅者分别处理消息

事件总线的作用是解耦模块，将命令式编程改为事件驱动架构。

关于消息顺序：
- 当订阅者协程数为 1 且处理函数始终返回 nil 时，NSQite 保证消息有序
- 其他情况下（并发处理或消息重试），NSQite 无法保证消息顺序

```go
type Reader1 struct{}

// HandleMessage implements Handler.
func (r *Reader1) HandleMessage(message *EventMessage[string]) error {
	time.Sleep(time.Second)
	fmt.Println("reader one :", message.Body)
	return nil
}

// 模拟一个作者疯狂写书，出版社派出 5 个编辑，每个编辑每秒只能处理一本书
func main() {
	const topic = "a-book"
	p := NewPublisher[string]()
	// 限制任务失败重试次数 10 次
	c := NewSubscriber(topic, "comsumer1", WithMaxAttempts[string](10))
	c.AddConcurrentHandlers(&Reader1{}, 5)

	for i := 0; i < 5; i++ {
		// 此函数会返回 err，正常使用发布订阅不会出错，可以直接丢弃 err 不处理
		p.Publish(topic, fmt.Sprintf("a >> hello %d", i))
	}

	time.Sleep(2 * time.Second)
}

```

手动完成
```go
type Reader3 struct {
	receivedMessages sync.Map
	attemptCount     int32
}

// HandleMessage implements Handler.
func (r *Reader3) HandleMessage(message *EventMessage[string]) error {
	// 禁用自动完成
	message.DisableAutoResponse()
	if message.Body == "hello" || message.Attempts > 3 {
		// 手动完成
		r.receivedMessages.Store(message.Body, true)
		message.Finish()
		return nil
	}
	// 手动延迟 1 秒后重试
	atomic.AddInt32(&r.attemptCount, 1)
	message.Requeue(time.Second)
	return nil
}
```

### 事务消息（开发中）

基于数据库实现，支持 GORM，支持事务发布消息，由生产者与消费者组成。

适用场景：
+ 单体架构或分布式架构
+ 消息与数据库事务绑定，事务回滚时消息可撤销
+ 单体架构中消息快速处理
+ 分布式架构中消息延迟 100~5000 毫秒

示例场景：
**删除用户时，需要同时删除用户相关的数据**

1. 用户资料模块订阅删除用户主题
2. 在删除用户的事务中，发布事务消息
3. 事务提交后，消费者收到消息开始处理
4. 如果处理过程中服务器崩溃
5. 重启后消费者会重新收到消息并处理

注意：消息可能会被多次触发，消费者需要实现幂等性处理。

### 代码示例

#### 基本使用
```go
type Reader1 struct{}

// HandleMessage implements Handler.
func (r *Reader1) HandleMessage(message *EventMessage[string]) error {
	time.Sleep(time.Second)
	fmt.Println("reader one :", message.Body)
	return nil
}

// 模拟一个作者疯狂写书，出版社派出 5 个编辑，每个编辑每秒只能处理一本书
func main() {
	const topic = "a-book"
	p := NewPublisher[string]()
	// 限制任务失败重试次数 10 次
	c := NewSubscriber(topic, "comsumer1", WithMaxAttempts[string](10))
	c.AddConcurrentHandlers(&Reader1{}, 5)

	for i := 0; i < 5; i++ {
		// 此函数会返回 err，正常使用发布订阅不会出错，可以直接丢弃 err 不处理
		p.Publish(topic, fmt.Sprintf("a >> hello %d", i))
	}

	time.Sleep(2 * time.Second)
}
```

#### 手动控制消息处理
```go
type Reader3 struct {
	receivedMessages sync.Map
	attemptCount     int32
}

// HandleMessage implements Handler.
func (r *Reader3) HandleMessage(message *EventMessage[string]) error {
	// 禁用自动完成
	message.DisableAutoResponse()
	if message.Body == "hello" || message.Attempts > 3 {
		// 手动完成
		r.receivedMessages.Store(message.Body, true)
		message.Finish()
		return nil
	}
	// 手动延迟 1 秒后重试
	atomic.AddInt32(&r.attemptCount, 1)
	message.Requeue(time.Second)
	return nil
}
```

### 维护与优化

NSQite 使用 slog 记录日志，如果出现以下警告日志，需要及时优化参数：

- `[NSQite] publish message timeout`：表示发布速度过快，消费者处理不过来。可以通过以下方式优化：
  - 增加缓存队列长度
  - 增加并发处理协程数
  - 优化消费者处理函数性能

默认超时时间为 3 秒，如果频繁出现超时，可以通过 `WithCheckTimeout(10*time.Second)` 调整超时时间。
