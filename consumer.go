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
	topic, channel      string
	incomingMessages    chan *Message
	incomingMessagesMap map[int]struct{}
	incomingMutex       sync.RWMutex

	backendMessage chan *Message

	queueSize uint16

	stopHandler     sync.Once
	runningHandlers int32

	MaxAttempts uint32

	pendingMessages int32

	log *slog.Logger

	deferredPQ    pqueue.PriorityQueue
	deferredMutex sync.Mutex

	exit chan struct{}
}

// MessageHandler 定义消息处理函数类型
type MessageHandler func(msg *Message) error

type ConsumerOption func(*Consumer)

// WithQueueSize 设置消息队列大小
// WithQueueSize(0) 表示无缓冲队列
func WithConsumerQueueSize(size uint16) ConsumerOption {
	return func(c *Consumer) {
		c.queueSize = size
	}
}

// WithMaxAttempts 设置消息最大重试次数，其次数是最终回调函数执行次数
func WithConsumerMaxAttempts(maxAttempts uint32) ConsumerOption {
	return func(c *Consumer) {
		c.MaxAttempts = maxAttempts
	}
}

// NewConsumer 创建一个新的Consumer
func NewConsumer(topic, channel string, opts ...ConsumerOption) *Consumer {
	c := Consumer{
		topic:               topic,
		channel:             channel,
		log:                 slog.Default().With("topic", topic, "channel", channel),
		MaxAttempts:         10,
		queueSize:           128,
		deferredPQ:          pqueue.New(8),
		exit:                make(chan struct{}),
		incomingMessagesMap: make(map[int]struct{}),
	}
	for _, opt := range opts {
		opt(&c)
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
	var ok bool
	for {
		select {
		case msg, ok = <-c.incomingMessages:
		case msg, ok = <-c.backendMessage:
		case <-c.exit:
			return
		}
		if !ok {
			return
		}

		attempts := atomic.AddUint32(&msg.Attempts, 1)
		if c.shouldFailMessage(attempts) {
			msg.Finish()
			continue
		}

		atomic.AddInt32(&c.pendingMessages, 1)
		err := handler(msg)
		atomic.AddInt32(&c.pendingMessages, -1)
		if err != nil {
			if !msg.IsAutoResponseDisabled() {
				// 允许脏读，因为其长度准确性不重要
				l := c.deferredPQ.Len()
				msg.Requeue(time.Second + time.Duration(l)*10*time.Millisecond)
			}
			continue
		}
		if !msg.IsAutoResponseDisabled() {
			msg.Finish()
		}
	}
}

func (c *Consumer) shouldFailMessage(attempts uint32) bool {
	if c.MaxAttempts > 0 && attempts > c.MaxAttempts {
		c.log.Warn("[NSQite] message attempts limit reached", "attempts", attempts)
		return true
	}
	return false
}

func (c *Consumer) backendLoop() {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

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

func (c *Consumer) sendMessage(msg *Message) error {
	c.incomingMutex.Lock()
	_, ok := c.incomingMessagesMap[msg.ID]
	if ok {
		c.incomingMutex.Unlock()
		return nil
	}
	c.incomingMessagesMap[msg.ID] = struct{}{}
	c.incomingMutex.Unlock()
	return c.SendMessage(msg)
}

func (c *Consumer) SendMessage(msg *Message) error {
	msg.Delegate = c
	c.incomingMessages <- msg
	return nil
}

// OnFinish implements MessageDelegate.
func (c *Consumer) OnFinish(msg *Message) {
	c.log.Debug("[NSQite] message finished", "msgID", msg.ID)

	c.incomingMutex.Lock()
	delete(c.incomingMessagesMap, msg.ID)
	c.incomingMutex.Unlock()

	if err := TransactionMQ().Finish(msg, c.channel); err != nil {
		c.log.Error("[NSQite] finish message", "err", err)
	}
}

// OnRequeue implements MessageDelegate.
func (c *Consumer) OnRequeue(m *Message, delay time.Duration) {
	c.log.Debug("[NSQite] message requeue", "msgID", m.ID, "delay", delay, "attempts", m.Attempts)

	if delay <= 0 {
		c.SendMessage(m)
		return
	}
	c.StartDeferredTimeout(m, delay)
}

// OnTouch implements MessageDelegate.
func (c *Consumer) OnTouch(*Message) {
	c.log.Debug("[NSQite] message touch")
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
func (c *Consumer) WaitMessage() {
	for {
		time.Sleep(100 * time.Millisecond)
		c.deferredMutex.Lock()
		l := c.deferredPQ.Len()
		c.deferredMutex.Unlock()
		if l == 0 && len(c.incomingMessages) == 0 && atomic.LoadInt32(&c.pendingMessages) == 0 {
			return
		}
	}
}

func (c *Consumer) Stop() {
	TransactionMQ().DelConsumer(c)
	c.stopHandler.Do(func() {
		close(c.exit)
		close(c.incomingMessages)
	})
}
