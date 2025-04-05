# nsqite

[中文](./README_CN.md)  [英语](./README.md)

Fast and reliable background jobs in Go，消息队列，支持 sqlite,postgres,orm 做持久化存储。

## 介绍

你可以使用 channel，nsq, pulsar 等消息队列很好的处理问题。

但项目刚开始很有可能并不是千万级并发量，只需要一个因地制宜，满足消息队列的功能的组件。

可能项目是从 sqlite 开始，慢慢扩展到 postgresql，引入 redis 等。

可以是 sqlite 做消息队列持久化，可以是 postgresql 做消息队列持久化，也可以是 redis 做消息队列持久化。

nsqite 使用方式与 go-nsq 使用方式类似，其实现参考的 go-nsq，也期望未来你的项目迁移到 nsq 来承载千万级并发。

## 快速开始

```go
type TestHandler struct{}

func (h *TestHandler) HandleMessage(message *Message) error {
	fmt.Println(string(message.Body))
	return nil
}

func TestSimple(t *testing.T) {
	producter := NewProducter(100)
	consumer := NewConsumer("test", "c1")
	consumer.AddConcurrentHandlers(&TestHandler{}, 1)

	producter.Publish("test", []byte("hello"))
	time.Sleep(1 * time.Second)
}
```