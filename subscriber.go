package nsqite

import (
	"container/heap"
	"context"
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

// Subscriber 消息订阅者
type Subscriber[T any] struct {
	*BaseConfig
	topic, channel   string
	incomingMessages chan *EventMessage[T]
	backendMessage   chan *EventMessage[T]

	stopHandler     sync.Once
	runningHandlers int32

	pendingMessages int32
	exit            chan struct{}

	deferredPQ    pqueue.PriorityQueue
	deferredMutex sync.Mutex
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
func (s *Subscriber[T]) SendMessage(ctx context.Context, msg interface{}) error {
	typedMsg, ok := msg.(*EventMessage[T])
	if !ok {
		panic("message type not supported")
	}
	typedMsg.Delegate = s

	var recordedLog bool

	timer := getTimer(s.checkTimeout)
	defer putTimer(timer)
	for {
		select {
		case s.incomingMessages <- typedMsg:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			// 避免重复打印日志
			if !recordedLog {
				s.log().Warn("[NSQite] publish message timeout", "msgID", typedMsg.ID, "queueSize", len(s.incomingMessages))
				recordedLog = true
			}
			if s.discardOnBlocking {
				return nil
			}
			continue
		}
	}
}

// NewSubscriber 创建消息订阅者
func NewSubscriber[T any](topic, channel string, opts ...Option) *Subscriber[T] {
	c := Subscriber[T]{
		topic:      topic,
		channel:    channel,
		deferredPQ: pqueue.New(8),
		exit:       make(chan struct{}),
		BaseConfig: newBaseConfig(),
	}
	for _, opt := range opts {
		opt(c.BaseConfig)
	}
	c.incomingMessages = make(chan *EventMessage[T], c.queueSize)
	c.backendMessage = make(chan *EventMessage[T], c.queueSize/4+1)
	eventBus.AddSubscriber(&c)
	return &c
}

// AddConcurrentHandlers 添加并发处理程序
func (s *Subscriber[T]) AddConcurrentHandlers(handler EventHandler[T], concurrency int32) {
	if !atomic.CompareAndSwapInt32(&s.runningHandlers, 0, concurrency) {
		panic("concurrent handlers already set")
	}
	atomic.AddInt32(&s.runningHandlers, concurrency)
	go s.backendLoop()
	for range concurrency {
		go s.handlerLoop(handler)
	}
}

func (s *Subscriber[T]) backendLoop() {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
		case <-s.exit:
			return
		}
		for {
			s.deferredMutex.Lock()
			item, _ := s.deferredPQ.PeekAndShift(time.Now().UnixMilli())
			s.deferredMutex.Unlock()
			if item == nil {
				timer.Reset(time.Second)
				break
			}
			msg := item.Value.(*EventMessage[T])
			s.backendMessage <- msg
			timer.Reset(time.Second)
		}
	}
}

// handlerLoop 处理消息的循环
func (s *Subscriber[T]) handlerLoop(handler EventHandler[T]) {
	defer func() {
		if count := atomic.AddInt32(&s.runningHandlers, -1); count == 0 {
			s.Stop()
		}
	}()

	var msg *EventMessage[T]
	var ok bool
	for {
		select {
		case msg, ok = <-s.incomingMessages:
		case msg, ok = <-s.backendMessage:
		case <-s.exit:
			return
		}
		if !ok {
			return
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
				// 允许脏读，因为其长度准确性不重要
				l := s.deferredPQ.Len() + 1
				msg.Requeue(time.Second + time.Duration(msg.Attempts)*200*time.Millisecond + time.Duration(l)*10*time.Millisecond)
			}
			continue
		}
		if !msg.IsAutoResponseDisabled() {
			msg.Finish()
		}
	}
}

func (s *Subscriber[T]) log() *slog.Logger {
	return slog.Default().With("topic", s.topic, "channel", s.channel)
}

func (s *Subscriber[T]) shouldFailMessage(msg *EventMessage[T]) bool {
	if s.maxAttempts > 0 && msg.Attempts > s.maxAttempts {
		s.log().Warn("message attempts limit reached", "attempts", msg.Attempts, "id", msg.ID)
		return true
	}
	return false
}

// Stop 订阅者停止接收消息
func (s *Subscriber[T]) Stop() {
	eventBus.DelConsumer(s)
	s.stopHandler.Do(func() {
		close(s.exit)
		close(s.incomingMessages)
	})
}

// OnFinish implements MessageDelegate.
func (s *Subscriber[T]) OnFinish(_ *EventMessage[T]) {
}

// OnRequeue implements MessageDelegate.
func (s *Subscriber[T]) OnRequeue(m *EventMessage[T], delay time.Duration) {
	s.log().Debug("message requeue", "msgID", m.ID, "delay", delay)

	if delay <= 0 {
		_ = s.SendMessage(context.Background(), m)
		return
	}

	s.StartDeferredTimeout(m, delay)
}

// OnTouch implements MessageDelegate.
func (s *Subscriber[T]) OnTouch(*EventMessage[T]) {
}

// StartDeferredTimeout 开始延迟处理消息
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

// WaitMessage waits for all messages to be processed, convenient for testing message processing progress, not recommended for use in production environments
// 等待所有消息处理完成，方便测试消息处理进度，不建议在生产环境中使用
func (s *Subscriber[T]) WaitMessage() {
	for {
		time.Sleep(100 * time.Millisecond)
		s.deferredMutex.Lock()
		l := s.deferredPQ.Len()
		s.deferredMutex.Unlock()
		if l == 0 && len(s.incomingMessages) == 0 && atomic.LoadInt32(&s.pendingMessages) == 0 {
			return
		}
	}
}

// Wait 等待订阅者执行 stop 方法
func (s *Subscriber[T]) Wait() {
	<-s.exit
	s.WaitMessage()
}

// SubscriberHandler 是一个适配器，允许使用普通函数作为 EventHandler
type SubscriberHandlerFunc[T any] func(message *EventMessage[T]) error

func (s SubscriberHandlerFunc[T]) HandleMessage(msg *EventMessage[T]) error {
	return s(msg)
}
