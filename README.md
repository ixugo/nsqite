# NSQite

[中文](./README.md) [更多语言待支持](./README.md)

Fast and reliable background jobs in Go，消息队列，支持 sqlite,postgres,orm 做持久化存储。

## 介绍

你可以使用 channel，nsq, pulsar 等消息队列很好的处理问题。

但项目刚开始很有可能并不是千万级并发量，只需要一个因地制宜，满足消息队列的功能的组件。

可能项目是从 sqlite 开始，慢慢扩展到 postgresql，引入 redis 等。

可以是 sqlite 做消息队列持久化，可以是 postgresql 做消息队列持久化，也可以是 redis 做持久化。

NSQite 使用方式与 go-nsq 使用方式类似，其实现参考的 go-nsq，也期望未来你的项目迁移到 nsq 来承载百万并发。

NSQite 保证消息至少被传递一次 ，尽管可能出现重复消息。消费者应该预料到这一点，并进行重复数据删除或幂等操作。

![](./docs/1.gif)

## 快速开始

### 事件总线

适用于当单体架构中的业务场景，基于内存实现，发布者与订阅者 1:N 关系，含任务失败延迟重试

我是否需要 **事件总线**?

+ 单体架构
+ 事件发生时，订阅者快速知晓
+ 有补偿机制，例如服务重启内存丢失后，发布者会根据需求重新触发事件
+ 消息体支持泛型

案例:
**系统中发生了告警，一边要记录到数据库，一边要通过 websocket 通知客户端发生了什么**
1. 告警记录数据库，订阅主题
2. websocket 客户端通知，订阅主题
3. 告警发生，发布者发布告警消息
4. 两个订阅者分别收到消息，处理消息

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

### 事务消息(后台任务，基于数据库持久化，开发中...)

基于数据库实现，支持 GORM，支持事务发布消息，由生产者与消费者组成

如果有不使用 gorm 且使用此项目的用户，你可以提出 issus，让我们一起想点办法不依赖 gorm。

为什么消息队列没有考虑使用泛型? 需要持久化，由调用者选择序列化/反序列化方式，消息全部按字节数组存储。

我是否需要 **事务消息**?

+ 单体架构 or 分布式架构
+ 消息跟数据库事务绑定，当事务回滚时，消息发布可撤销，消费者可触发重试
+ 非事务消息在单体架构中会极速处理
+ 在分布式架构中会有 100~5000 毫秒随机延迟

案例:
**删除用户时，连带删除用户相关的数据。**
1. 其它用户资料模块订阅主题
2. 删除用户的事务中，发布 nsqite 事务消息，当事务 commit 后，消费者会收到消息进行处理
3. 服务器断电重启恢复
4. 消费者收到消息，处理删除用户资料的事务中，程序崩溃重启
5. 消费者再次收到消息，处理完成，标记为完成

可以看到在案例中，消息有概率触发多次，消费者回调函数要好做幂等性，允许消息重复消费。

在删除用户的事务完成后，发布非事务消息时，服务器有概率崩溃，此时消息并没有记录，则也无法连带删除用户相关数据，各有各的应用场景，你明白为什么需要事务消息了吗?


如果 **事件总线** 和 **事务消息** 都无法满足需求，恭喜你项目已做大做强，可以考虑考虑 nsq，pulsar，kafka 了。

```go
p := NewProducer(10)
c := NewConsumer(topic, "comsumer1")
```

### 维护与优化

内置了使用 slog 记录日志，如果遇到以下 Warn 级别日志，应及时优化参数

`[NSQite] publish message timeout` 说明发布太快，消费者跟不上，参数调优可以增加缓存队列长度，或增加并发处理协程，默认参数一般是够的，也许应该考虑是不是消费者回调函数还可以更快一些。

超时时间，默认是 3 秒，如果发生超时，这可能会导致日志记录非常频繁，可以在创建订阅者时 `WithCheckTimeout(10*time.Second)` 来调整。
