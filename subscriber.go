package nsqite

import (
	"container/heap"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ixugo/nsqite/pqueue"
)

var (
	_ SubscriberInfo               = (*Subscriber[string])(nil)
	_ EventMessageDelegate[string] = (*Subscriber[string])(nil)
)

type Subscriber[T any] struct {
	topic, channel   string
	incomingMessages chan *EventMessage[T]
	stopHandler      sync.Once
	runningHandlers  int32

	MaxAttempts uint32

	log *slog.Logger

	queueSize uint16

	pendingMessages int32

	deferredPQ    pqueue.PriorityQueue
	deferredMutex sync.Mutex
}

type SubscriberOption[T any] func(*Subscriber[T])

func WithQueueSize[T any](size uint16) SubscriberOption[T] {
	return func(s *Subscriber[T]) {
		s.queueSize = size
	}
}

func WithMaxAttempts[T any](maxAttempts uint32) SubscriberOption[T] {
	return func(s *Subscriber[T]) {
		s.MaxAttempts = maxAttempts
	}
}

// GetChannel implements SubscriberInfo.
func (s *Subscriber[T]) GetChannel() string {
	return s.channel
}

// GetTopic implements SubscriberInfo.
func (s *Subscriber[T]) GetTopic() string {
	return s.topic
}

// SendMessage implements SubscriberInfo.
func (s *Subscriber[T]) SendMessage(msg interface{}) {
	if typedMsg, ok := msg.(*EventMessage[T]); ok {
		typedMsg.Delegate = s
		s.incomingMessages <- typedMsg
	} else {
		panic("message type not supported")
	}
}

func NewSubscriber[T any](topic, channel string, opts ...SubscriberOption[T]) *Subscriber[T] {
	c := Subscriber[T]{
		topic:       topic,
		channel:     channel,
		log:         slog.Default().With("topic", topic, "channel", channel),
		queueSize:   256,
		deferredPQ:  pqueue.New(8),
		MaxAttempts: 0,
	}
	for _, opt := range opts {
		opt(&c)
	}
	c.incomingMessages = make(chan *EventMessage[T], c.queueSize)
	eventBus.AddConsumer(&c)
	return &c
}

func (s *Subscriber[T]) AddConcurrentHandlers(handler EventHandler[T], concurrency int32) {
	atomic.AddInt32(&s.runningHandlers, concurrency)
	for range concurrency {
		go s.handlerLoop(handler)
	}
}

func (s *Subscriber[T]) handlerLoop(handler EventHandler[T]) {
	defer func() {
		if count := atomic.AddInt32(&s.runningHandlers, -1); count == 0 {
			s.Stop()
		}
	}()

	const interval = 200 * time.Millisecond
	timer := time.NewTimer(interval)
	defer timer.Stop()

	var msg *EventMessage[T]
	var ok bool
	for {
		select {
		case msg, ok = <-s.incomingMessages:
			if !ok {
				return
			}
		case <-timer.C:
			timer.Reset(interval)
			item, _ := s.deferredPQ.PeekAndShift(time.Now().UnixMilli())
			if item == nil {
				continue
			}
			msg = item.Value.(*EventMessage[T])
		}

		atomic.AddUint32(&msg.Attempts, 1)
		if s.shouldFailMessage(msg) {
			msg.Finish()
			continue
		}

		atomic.AddInt32(&s.pendingMessages, 1)
		err := handler.HandleMessage(msg)
		atomic.AddInt32(&s.pendingMessages, -1)
		if err != nil {
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

func (s *Subscriber[T]) shouldFailMessage(msg *EventMessage[T]) bool {
	if s.MaxAttempts > 0 && msg.Attempts > s.MaxAttempts {
		s.log.Warn("message attempts limit reached", "attempts", msg.Attempts, "msgID", msg.ID)
		return true
	}
	return false
}

func (s *Subscriber[T]) Stop() {
	eventBus.DelConsumer(s)
	s.stopHandler.Do(func() {
		close(s.incomingMessages)
	})
}

// OnFinish implements MessageDelegate.
func (s *Subscriber[T]) OnFinish(msg *EventMessage[T]) {
	slog.Debug("message finished", "msgID", msg.ID)
}

// OnRequeue implements MessageDelegate.
func (s *Subscriber[T]) OnRequeue(m *EventMessage[T], delay time.Duration) {
	slog.Debug("message requeue", "msgID", m.ID, "delay", delay)

	if delay == 0 {
		s.SendMessage(m)
		return
	}

	s.StartDeferredTimeout(m, delay)
}

// OnTouch implements MessageDelegate.
func (s *Subscriber[T]) OnTouch(*EventMessage[T]) {
}

func (s *Subscriber[T]) StartDeferredTimeout(msg *EventMessage[T], delay time.Duration) {
	absTs := time.Now().Add(delay).UnixMilli()
	item := &pqueue.Item{
		Value:    msg,
		Priority: absTs,
	}
	s.addToDeferredPQ(item)
}

func (s *Subscriber[T]) addToDeferredPQ(item *pqueue.Item) {
	s.deferredMutex.Lock()
	heap.Push(&s.deferredPQ, item)
	s.deferredMutex.Unlock()
}

// Wait 等待所有消息处理完成，方便测试消息处理进度，不建议在生产环境中使用
func (s *Subscriber[T]) Wait() {
	for {
		s.deferredMutex.Lock()
		l := s.deferredPQ.Len()
		s.deferredMutex.Unlock()
		if l == 0 && len(s.incomingMessages) == 0 && atomic.LoadInt32(&s.pendingMessages) == 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
