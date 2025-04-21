package nsqite

import (
	"container/heap"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ixugo/nsqite/pqueue"
)

// Consumer 表示一个消息消费者
type Consumer struct {
	*BaseConfig
	topic, channel      string
	incomingMessages    chan *Message
	incomingMessagesMap map[int]struct{}
	incomingMutex       sync.RWMutex

	backendMessage chan *Message

	stopHandler     sync.Once
	runningHandlers int32

	pendingMessages int32

	deferredPQ    pqueue.PriorityQueue
	deferredMutex sync.Mutex

	exit chan struct{}
}

// MessageHandler 定义消息处理函数类型
type MessageHandler interface {
	HandleMessage(msg *Message) error
}

// NewConsumer 创建一个新的Consumer
func NewConsumer(topic, channel string, opts ...Option) *Consumer {
	c := Consumer{
		topic:               topic,
		channel:             channel,
		deferredPQ:          pqueue.New(8),
		exit:                make(chan struct{}),
		incomingMessagesMap: make(map[int]struct{}),
		BaseConfig:          newBaseConfig(),
	}
	for _, opt := range opts {
		opt(c.BaseConfig)
	}
	c.incomingMessages = make(chan *Message, c.queueSize)
	c.backendMessage = make(chan *Message, c.queueSize/4+1)
	TransactionMQ().AddConsumer(&c)
	return &c
}

// AddConcurrentHandlers 添加并发处理程序
func (c *Consumer) AddConcurrentHandlers(handler MessageHandler, concurrency int32) {
	if !atomic.CompareAndSwapInt32(&c.runningHandlers, 0, concurrency) {
		panic("concurrent handlers already set")
	}
	atomic.AddInt32(&c.runningHandlers, concurrency)
	go c.backendLoop()
	for range concurrency {
		go c.handlerLoop(handler)
	}
}

// handlerLoop 处理消息的循环
func (c *Consumer) handlerLoop(handler MessageHandler) {
	defer func() {
		if count := atomic.AddInt32(&c.runningHandlers, -1); count == 0 {
			c.Stop()
		}
	}()

	var msg *Message
	for {
		select {
		case msg = <-c.incomingMessages:
			if msg == nil {
				return
			}
		case msg = <-c.backendMessage:
			if msg == nil {
				continue
			}
		}

		atomic.AddUint32(&msg.Attempts, 1)
		if c.shouldFailMessage(msg) {
			msg.Finish()
			continue
		}

		atomic.AddInt32(&c.pendingMessages, 1)
		err := handler.HandleMessage(msg)
		atomic.AddInt32(&c.pendingMessages, -1)
		if err != nil {
			if !msg.IsAutoResponseDisabled() {
				// 允许脏读，因为其长度准确性不重要
				l := c.deferredPQ.Len() + 1
				msg.Requeue(time.Second + time.Duration(msg.Attempts)*200*time.Millisecond + time.Duration(l)*10*time.Millisecond)
			}
			continue
		}
		if !msg.IsAutoResponseDisabled() {
			msg.Finish()
		}
	}
}

func (c *Consumer) log() *slog.Logger {
	return slog.Default().With("topic", c.topic, "channel", c.channel)
}

func (c *Consumer) shouldFailMessage(msg *Message) bool {
	if c.maxAttempts > 0 && msg.Attempts > c.maxAttempts {
		c.log().Warn("[NSQite] message attempts limit reached", "attempts", msg.Attempts, "id", msg.ID)
		return true
	}
	return false
}

func (c *Consumer) backendLoop() {
	timer := time.NewTimer(100 * time.Millisecond)
	defer func() {
		timer.Stop()
		close(c.backendMessage)
		c.deferredMutex.Lock()
		c.deferredPQ = c.deferredPQ[:0]
		c.deferredMutex.Unlock()
	}()

	for {
		select {
		case <-timer.C:
		case <-c.exit:
			return
		}
		for {
			c.deferredMutex.Lock()
			item, _ := c.deferredPQ.PeekAndShift(time.Now().UnixMilli())
			c.deferredMutex.Unlock()
			if item == nil {
				timer.Reset(time.Second)
				break
			}
			msg := item.Value.(*Message)
			c.backendMessage <- msg
			timer.Reset(time.Second)
		}
	}
}

func (c *Consumer) sendMessage(msg Message) error {
	c.incomingMutex.Lock()
	_, ok := c.incomingMessagesMap[msg.ID]
	if ok {
		c.incomingMutex.Unlock()
		return nil
	}
	c.incomingMessagesMap[msg.ID] = struct{}{}
	c.incomingMutex.Unlock()
	return c.SendMessage(&msg)
}

func (c *Consumer) SendMessage(msg *Message) error {
	msg.Delegate = c

	var recordedLog bool
	timer := getTimer(c.checkTimeout)
	defer putTimer(timer)
	for {
		select {
		case c.incomingMessages <- msg:
			return nil
		case <-timer.C:
			// 避免重复打印日志
			if !recordedLog {
				c.log().Warn("[NSQite] publish message timeout", "msgID", msg.ID, "queueSize", len(c.incomingMessages))
				recordedLog = true
			}
			continue
		}
	}
}

// OnFinish implements MessageDelegate.
func (c *Consumer) OnFinish(msg *Message) {
	c.log().Debug("[NSQite] message finished", "msgID", msg.ID)

	c.incomingMutex.Lock()
	delete(c.incomingMessagesMap, msg.ID)
	c.incomingMutex.Unlock()

	if err := TransactionMQ().Finish(msg, c.channel); err != nil {
		c.log().Error("[NSQite] finish message", "err", err)
	}
}

// OnRequeue implements MessageDelegate.
func (c *Consumer) OnRequeue(m *Message, delay time.Duration) {
	c.log().Debug("[NSQite] message requeue", "msgID", m.ID, "delay", delay, "attempts", m.Attempts)

	if delay <= 0 {
		_ = c.SendMessage(m)
		return
	}
	c.StartDeferredTimeout(m, delay)
}

// OnTouch implements MessageDelegate.
func (c *Consumer) OnTouch(*Message) {
	c.log().Debug("[NSQite] message touch")
}

// StartDeferredTimeout 开始延迟处理消息
func (c *Consumer) StartDeferredTimeout(msg *Message, delay time.Duration) {
	absTs := time.Now().Add(delay).UnixMilli()
	item := &pqueue.Item{
		Value:    msg,
		Priority: absTs,
	}
	c.addToDeferredPQ(item)
}

func (c *Consumer) addToDeferredPQ(item *pqueue.Item) {
	c.deferredMutex.Lock()
	heap.Push(&c.deferredPQ, item)
	c.deferredMutex.Unlock()
}

// WaitMessage 等待所有消息处理完成，方便测试消息处理进度，不建议在生产环境中使用
func (c *Consumer) WaitMessage() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		for {
			time.Sleep(100 * time.Millisecond)
			c.deferredMutex.Lock()
			l := c.deferredPQ.Len()
			c.deferredMutex.Unlock()
			if l == 0 && len(c.incomingMessages) == 0 && atomic.LoadInt32(&c.pendingMessages) == 0 {
				close(done)
				return
			}
		}
	}()
	return done
}

// Stop the consumers will not receive messages again
// 1. Stop receiving new messages
// 2. Clear the deferred retry queue data
// 3. Gradually stop the goroutines started by the current consumer
func (c *Consumer) Stop() {
	TransactionMQ().DelConsumer(c.topic, c.channel)
	c.stopHandler.Do(func() {
		close(c.exit)
		close(c.incomingMessages)
	})
}

// ConsumerHandler 提供一个默认的 MessageHandler 实现，放入函数即可
type ConsumerHandlerFunc func(msg *Message) error

func (h ConsumerHandlerFunc) HandleMessage(msg *Message) error {
	return h(msg)
}
