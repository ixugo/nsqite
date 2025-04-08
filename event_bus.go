package nsqite

import "sync"

// SubscriberInfo 定义了获取订阅者信息的接口
type SubscriberInfo interface {
	GetTopic() string
	GetChannel() string
	SendMessage(msg interface{})
}

var eventBus = NewEventBus()

type EventBus struct {
	consumers map[string]map[string]SubscriberInfo
	m         sync.RWMutex
}

func NewEventBus() *EventBus {
	return &EventBus{
		consumers: make(map[string]map[string]SubscriberInfo),
	}
}

func (s *EventBus) AddConsumer(c SubscriberInfo) {
	s.m.Lock()
	defer s.m.Unlock()

	topic := c.GetTopic()
	channel := c.GetChannel()

	chs := s.consumers[topic]
	if chs == nil {
		chs = make(map[string]SubscriberInfo)
		s.consumers[topic] = chs
	}
	chs[channel] = c
}

func (s *EventBus) DelConsumer(c SubscriberInfo) {
	s.m.Lock()
	defer s.m.Unlock()

	topic := c.GetTopic()
	channel := c.GetChannel()

	delete(s.consumers[topic], channel)
}

func (s *EventBus) Publish(topic string, msg interface{}) {
	s.m.RLock()
	defer s.m.RUnlock()

	consumers, ok := s.consumers[topic]
	if !ok {
		return
	}

	for _, c := range consumers {
		c.SendMessage(msg)
	}
}
