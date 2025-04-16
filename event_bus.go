package nsqite

import (
	"context"
	"fmt"
	"sync"
)

// SubscriberInfo 定义了获取订阅者信息的接口
type SubscriberInfo interface {
	GetTopic() string
	GetChannel() string
	SendMessage(ctx context.Context, msg interface{}) error
}

var eventBus = NewEventBus()

type EventBus struct {
	subscribers map[string]map[string]SubscriberInfo
	m           sync.RWMutex
}

func NewEventBus() *EventBus {
	return &EventBus{
		subscribers: make(map[string]map[string]SubscriberInfo),
	}
}

func (s *EventBus) AddSubscriber(c SubscriberInfo) {
	s.m.Lock()
	defer s.m.Unlock()

	topic := c.GetTopic()
	channel := c.GetChannel()

	chs := s.subscribers[topic]
	if chs == nil {
		chs = make(map[string]SubscriberInfo)
		s.subscribers[topic] = chs
	}
	chs[channel] = c
}

func (s *EventBus) DelConsumer(c SubscriberInfo) {
	s.m.Lock()
	defer s.m.Unlock()

	topic := c.GetTopic()
	channel := c.GetChannel()

	delete(s.subscribers[topic], channel)
}

func (s *EventBus) Publish(ctx context.Context, topic string, msg interface{}) error {
	s.m.RLock()
	subs, ok := s.subscribers[topic]
	s.m.RUnlock()
	if !ok {
		return fmt.Errorf("topic %s need subscribers", topic)
	}

	for _, c := range subs {
		if err := c.SendMessage(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}
