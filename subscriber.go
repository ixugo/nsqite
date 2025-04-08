package nsqite

import (
	"log/slog"
	"sync"
	"sync/atomic"
)

type Subscriber[T any] struct {
	topic, channel   string
	incomingMessages chan *EventMessage[T]
	stopHandler      sync.Once
	runningHandlers  int32

	MaxAttempts uint16

	log *slog.Logger

	queueSize uint16
}

// 实现 SubscriberInfo 接口
func (s *Subscriber[T]) GetTopic() string {
	return s.topic
}

func (s *Subscriber[T]) GetChannel() string {
	return s.channel
}

func (s *Subscriber[T]) SendMessage(msg interface{}) {
	if typedMsg, ok := msg.(*EventMessage[T]); ok {
		s.incomingMessages <- typedMsg
	} else {
		panic("message type not supported")
	}
}

type SubscriberOption[T any] func(*Subscriber[T])

func WithQueueSize[T any](size uint16) SubscriberOption[T] {
	return func(s *Subscriber[T]) {
		s.queueSize = size
	}
}

func NewSubscriber[T any](topic, channel string, opts ...SubscriberOption[T]) *Subscriber[T] {
	c := Subscriber[T]{
		topic:     topic,
		channel:   channel,
		log:       slog.Default().With("topic", topic, "channel", channel),
		queueSize: 256,
	}
	for _, opt := range opts {
		opt(&c)
	}
	c.incomingMessages = make(chan *EventMessage[T], c.queueSize)
	eventBus.AddConsumer(&c)
	return &c
}

func (c *Subscriber[T]) AddConcurrentHandlers(handler EventHandler[T], concurrency int32) {
	atomic.AddInt32(&c.runningHandlers, concurrency)
	for range concurrency {
		go c.handlerLoop(handler)
	}
}

func (c *Subscriber[T]) handlerLoop(handler EventHandler[T]) {
	defer func() {
		if count := atomic.AddInt32(&c.runningHandlers, -1); count == 0 {
			c.Stop()
		}
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

func (c *Subscriber[T]) shouldFailMessage(msg *EventMessage[T]) bool {
	if c.MaxAttempts > 0 && msg.Attempts > c.MaxAttempts {
		c.log.Warn("message attempts limit reached", "attempts", msg.Attempts, "msgID", msg.ID)
		return true
	}
	return false
}

func (c *Subscriber[T]) Stop() {
	eventBus.DelConsumer(c)
	c.stopHandler.Do(func() {
		close(c.incomingMessages)
	})
}
