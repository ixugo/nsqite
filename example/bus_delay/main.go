package main

import (
	"fmt"
	"time"

	"github.com/ixugo/nsqite"
)

type AlertType string

type Handler struct{}

var i int

var now = time.Now()

// HandleMessage implements nsqite.EventHandler.
func (h Handler) HandleMessage(msg *nsqite.EventMessage[AlertType]) error {
	i++
	fmt.Println(">>> ", i, time.Since(now))
	msg.DisableAutoResponse()
	if i >= 2 {
		fmt.Println(">>> finish")
		msg.Finish()
		return nil
	}

	// 延迟 5 秒后重试
	msg.Requeue(time.Second * 5)
	return nil
}

// 考虑:
// 当某个订阅者阻塞时，其后创建的订阅者也会阻塞
// 发布，某个阻塞以后，优先发布到其它订阅者，最后再阻塞那一个
// 创建消费者的时候指定参数， true/false 来决定，当阻塞时，是否丢弃数据
// WithDiscardOnBlocking(true) 丢弃

func main() {
	// 事件总线

	pub := nsqite.NewPublisher[AlertType]()

	const topic = "alert"
	sub1 := nsqite.NewSubscriber[AlertType](topic, "ch1_websocket", nsqite.WithQueueSize(1024))
	sub1.AddConcurrentHandlers(new(Handler), 1)

	pub.Publish(topic, "123")

	// ctx, cancel := context.WithTimeout(3 * time.Second)
	// if err := pub.PublishWithContext(context.Background(), topic, "123"); err != nil {
	// }

	time.Sleep(10 * time.Second)
}
