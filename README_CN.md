
<p align="center">
    <img src="logo.webp" alt="NSQite Logo" width="550"/>
</p>

[English](./README.md) [中文](./README.md)

Go 语言实现的轻量级消息队列，支持 SQLite、PostgreSQL 和 ORM 作为持久化存储。

## 介绍

在项目初期，你可能不需要像 NSQ、NATs、Pulsar 这样的大型分布式消息队列系统。NSQite 提供了一个简单可靠的解决方案，满足基本的消息队列需求。

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
	// 1. SetGorm
	nsqite.SetGorm(db)

	const topic = "a-book"
	p := NewProducer[string]()
	// 限制任务失败重试次数 10 次
	c := NewConsumer(topic, "comsumer1", WithMaxAttempts(10))
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

### 事务消息

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
	fmt.Println("reader one :", message.Body)
	return nil
}

// 模拟一个作者疯狂写书，出版社派出 5 个编辑，每个编辑每秒只能处理一本书
func main() {
	// 1. SetGorm
	nsqite.SetGorm(db)

	const topic = "a-book"
	p := NewProducer[string]()
	// 限制任务失败重试次数 10 次
	c := NewConsumer(topic, "comsumer1", WithMaxAttempts(10))
	c.AddConcurrentHandlers(&Reader1{}, 5)
	for i := 0; i < 5; i++ {
		p.Publish(topic, fmt.Sprintf("a >> hello %d", i))
	}
	time.Sleep(2 * time.Second)
}
```


### 维护与优化

NSQite 使用 slog 记录日志，如果出现以下警告日志，需要及时优化参数：

- `[NSQite] publish message timeout`：表示发布速度过快，消费者处理不过来。可以通过以下方式优化：
  - 增加缓存队列长度
  - 增加并发处理协程数
  - 优化消费者处理函数性能

默认超时时间为 3 秒，如果频繁出现超时，可以通过 `WithCheckTimeout(10*time.Second)` 调整超时时间。

## Benchmark

**事件总线**

一个发布者，一个订阅者，每秒并发 300 百万
![](./docs/bus.webp)

**事务消息队列**

一个生产者，一个消费者，基于 sqlite 数据库的，就差强人意了，使用 postgresql 会有更好的表现
![](./docs/mq.webp)


## 下一步开发任务

- 事件总线支持 Redis 作为持久化存储，支持分布式
- 事务消息队列支持分布式，其消费者收到消息后要更新一下数据库即可


## QA

**a,b,c 三个订阅者，当 b 阻塞时，会发生什么?**
- a 正常收到消息
- b 阻塞，导致 c 收不到消息
- b 阻塞，导致发布者也阻塞

可以用 `WithDiscardOnBlocking(true)` 丢弃消息
可以用 `PublicWithContext(ctx, topic, message)` 来限制发布超时时间
可以用 `WithQueueSize(1024)` 来设置缓存队列长度
可以调整回调，使消费者更快处理任务

**使用事务消息，消息已发布，a,c 已完成任务，此时服务重启，b 未完成会怎样?**
- 服务重启，b 会重新收到消息继续处理
- a,c 因为已完成，不会收到消息

**任务执行失败，可以自定义延迟执行时间吗?**
- 可以，看[案例](./example/bus_delay/main.go)

**如果任务一直失败，达到最大超时次数会怎样?**
任务结束有 2 个判定标准
- 任务执行成功
- 任务达到最大执行次数
如果无限次执行可以使用 `WithMaxAttempts(0)`，默认重试 10 次，可以改为更多次数 `WithMaxAttempts(100)`

**`WithMaxAttempts(10)` 表示重试 10 次，如果一直失败，问回调一共执行了多少次?**
- 10 次

**事务消息会在数据库存储多久?**
- 自动删除 15 天前的**全部**消息
- 自动删除 7 天前**已完成**的消息
- 当表数据超过 1 万条时，自动删除 3 天前的**已完成**消息

需要自定义时间? 请提 pr 或 issus。

**在事件总线中，回调一直失败会阻塞队列吗?**
- 不会，会进入优先队列中，延迟处理
- 大量任务失败，会导致消息堆积在内存中，达到最大尝试次数时释放

**在事件总线中，发布的某个主题阻塞了，会影响其它主题发布吗?**
- 不会，主题之间互不影响

**为什么依赖 gorm ?**
- 数据库表创建，gorm 能自适应表结构变化，自动建表
- 要同时兼容 pgx,sqlite，没有直接的最佳方案
- 移除 gorm ，仅提供接口，需要由用户提供具体实现，无法开箱即用

如果有兴趣减少 nsqite 的依赖项，可以提 pr 或 issus。