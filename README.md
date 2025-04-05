# nsqite

[中文](./README_CN.md)  [英语](./README.md)

Fast and reliable background jobs in Go，消息队列，支持 sqlite,postgres,orm 做持久化存储。

## Introduce

You can use message queues such as channel, NSQ, and Pulsar to handle problems very well.

However, at the beginning of a project, it is highly unlikely that there will be a concurrent volume of tens of millions. You only need a component that is tailored to the specific situation and meets the functions of a message queue.

Maybe the project starts with SQLite, and then gradually expands to PostgreSQL, and Redis and other components are introduced.

SQLite can be used for message queue persistence, PostgreSQL can be used for message queue persistence, and Redis can also be used for message queue persistence.

The usage of NSQite is similar to that of go-nsq. Its implementation refers to go-nsq. It is also hoped that in the future, your project will be migrated to NSQ to handle a concurrent volume of tens of millions.

## Quick Start

`go get -u github.com/ixugo/nsqite`

```go
import "github.com/ixugo/nsqite"

type TestHandler struct{}

func (h *TestHandler) HandleMessage(message *nsqite.Message) error {
	fmt.Println(string(message.Body))
	return nil
}

func TestSimple(t *testing.T) {
	producter := nsqite.NewProducter(100)
	consumer := nsqite.NewConsumer("test", "c1")
	consumer.AddConcurrentHandlers(&TestHandler{}, 1)

	producter.Publish("test", []byte("hello"))
	time.Sleep(1 * time.Second)
}
```