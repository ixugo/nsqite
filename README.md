# nsqite

[中文](./README_CN.md)  [英语](./README.md)

Fast and reliable background jobs in Go，消息队列，支持 sqlite,postgres,orm 做持久化存储。

## 介绍

你可以使用 channel，nsq, pulsar 等消息队列很好的处理问题。

但项目刚开始很有可能并不是千万级并发量，只需要一个因地制宜，满足消息队列的功能的组件。

可能项目是从 sqlite 开始，慢慢扩展到 postgresql，引入 redis 等。

可以是 sqlite 做消息队列持久化，可以是 postgresql 做消息队列持久化，也可以是 redis 做持久化。

nsqite 使用方式与 go-nsq 使用方式类似，其实现参考的 go-nsq，也期望未来你的项目迁移到 nsq 来承载百万级并发。

NSQite 保证消息至少被传递一次 ，尽管可能出现重复消息。消费者应该预料到这一点，并进行重复数据删除或幂等操作。

![](./docs/1.gif)

## 快速开始

### 事件总线

适用于当单体架构中的业务场景，基于内存实现，发布者与订阅者 1:N 关系，含延迟重试

适用于有补偿机制，例如服务重启后，发布者会根据需求重新触发事件

消息体支持泛型

关于处理顺序，当订阅者协程为 1 且订阅处理函数永远 return nil 时，nsqite 保证有序。
否则因并发和消息重试，nsqite 无法保证消息有序。

如果需要事务，服务重启后也必须执行，请考虑 **事务消息**。

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

### 事务消息(后台任务，基于数据库持久化)

基于数据库实现，支持 GORM，支持事务发布消息

为什么消息队列没有考虑使用泛型? 需要持久化，由调用者选择序列化/反序列化方式，消息全部按字节数组存储。

如果不考虑事务且有未执行到的补偿机制，请考虑 **事件总线**。

```go
p := NewProducer(10)
c := NewConsumer(topic, "comsumer1")
```
