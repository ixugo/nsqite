# nsqite

[中文](./README_CN.md)  [英语](./README.md)

Fast and reliable background jobs in Go，消息队列，支持 sqlite,postgres,orm 做持久化存储。

## 介绍

你可以使用 channel，nsq, pulsar 等消息队列很好的处理问题。

但项目刚开始很有可能并不是千万级并发量，只需要一个因地制宜，满足消息队列的功能的组件。

可能项目是从 sqlite 开始，慢慢扩展到 postgresql，引入 redis 等。

可以是 sqlite 做消息队列持久化，可以是 postgresql 做消息队列持久化，也可以是 redis 做消息队列持久化。

nsqite 使用方式与 go-nsq 使用方式类似，其实现参考的 go-nsq，也期望未来你的项目迁移到 nsq 来承载百万级并发。

NSQite 保证消息至少被传递一次 ，尽管可能出现重复消息。消费者应该预料到这一点，并进行重复数据删除或幂等操作。

![](./docs/1.gif)

## 快速开始

### 事件总线

适用于当单体架构中的业务场景，基于内存实现，发布者与订阅者 1:N 关系，含延迟重试

适用于有补偿机制，例如服务重启后，发布者会根据需求重新触发事件

消息体支持泛型

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
	c := NewSubscriber[string](topic, "comsumer1")
	c.AddConcurrentHandlers(&Reader1{}, 5)

	for i := 0; i < 5; i++ {
		p.Publish(topic, fmt.Sprintf("a >> hello %d", i))
	}

	time.Sleep(2 * time.Second)
}

```

### 事务消息(后台任务，基于数据库持久化)

基于数据库实现，支持 GORM，支持事务发布消息

```go
p := NewProducer(10)
c := NewConsumer(topic, "comsumer1")
```
