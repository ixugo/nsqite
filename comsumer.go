package nsqite

import (
	"log/slog"
	"sync"
	"sync/atomic"
)

type Consumer struct {
	topic, channel   string
	incomingMessages chan *Message
	stopHandler      sync.Once
	runningHandlers  int32

	MaxAttempts uint16

	log *slog.Logger
}

func NewConsumer(topic, channel string) *Consumer {
	c := Consumer{
		topic:            topic,
		channel:          channel,
		incomingMessages: make(chan *Message),
		log:              slog.Default().With("topic", topic, "channel", channel),
	}
	scheduler.AddConsumer(&c)
	return &c
}

func (c *Consumer) AddConcurrentHandlers(handler Handler, concurrency int) {
	atomic.AddInt32(&c.runningHandlers, int32(concurrency))
	for range concurrency {
		go c.handlerLoop(handler)
	}
}

func (c *Consumer) handlerLoop(handler Handler) {
	defer func() {
		atomic.AddInt32(&c.runningHandlers, -1)
	}()
	for {
		msg, ok := <-c.incomingMessages
		if !ok {
			return
		}
		if c.shouldFailMessage(msg) {
			msg.Finish()
			continue
		}
		if err := handler.HandleMessage(msg); err != nil {
			c.log.Error("message handle failed", "error", err, "msgID", msg.ID)
			if !msg.IsAutoResponseDisabled() {
				msg.Requeue(-1)
			}
			continue
		}
		if !msg.IsAutoResponseDisabled() {
			msg.Finish()
		}
	}
}

func (c *Consumer) shouldFailMessage(msg *Message) bool {
	if c.MaxAttempts > 0 && msg.Attempts > c.MaxAttempts {
		c.log.Warn("message attempts limit reached", "attempts", msg.Attempts, "msgID", msg.ID)
		return true
	}
	return false
}

func (c *Consumer) Stop() {
	scheduler.DelConsumer(c)
	c.stopHandler.Do(func() {
		close(c.incomingMessages)
	})
}
