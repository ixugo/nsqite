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

type cloner interface {
	Clone() cloner
}

var eventBus = newEventBus()

type EventBus struct {
	subscribers map[string]*subscriberMap
	m           sync.RWMutex
}
type subscriberMap struct {
	m    sync.RWMutex
	data map[string]SubscriberInfo
}

func (s *subscriberMap) add(channel string, c SubscriberInfo) {
	s.m.Lock()
	defer s.m.Unlock()
	s.data[channel] = c
}

func (s *subscriberMap) del(channel string) {
	s.m.Lock()
	defer s.m.Unlock()
	delete(s.data, channel)
}

func (s *subscriberMap) Len() int {
	s.m.RLock()
	defer s.m.RUnlock()
	return len(s.data)
}

func (s *subscriberMap) pub(ctx context.Context, msg cloner) error {
	s.m.RLock()
	defer s.m.RUnlock()

	var i int
	for _, c := range s.data {
		i++
		m := msg
		if i > 1 {
			m = msg.Clone()
		}
		if err := c.SendMessage(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

func newEventBus() *EventBus {
	return &EventBus{
		subscribers: make(map[string]*subscriberMap),
	}
}

func (s *EventBus) AddSubscriber(c SubscriberInfo) {
	s.m.Lock()
	defer s.m.Unlock()

	topic := c.GetTopic()
	channel := c.GetChannel()
	chs, ok := s.subscribers[topic]
	if !ok {
		chs = &subscriberMap{
			data: make(map[string]SubscriberInfo),
		}
		s.subscribers[topic] = chs
	}
	chs.add(channel, c)
}

func (s *EventBus) DelConsumer(c SubscriberInfo) {
	s.m.Lock()
	defer s.m.Unlock()

	topic := c.GetTopic()
	channel := c.GetChannel()

	chs, ok := s.subscribers[topic]
	if ok {
		chs.del(channel)
		if chs.Len() == 0 {
			delete(s.subscribers, topic)
		}
	}
}

func (s *EventBus) GetConsumer(topic string) (*subscriberMap, bool) {
	s.m.RLock()
	defer s.m.RUnlock()
	chs, ok := s.subscribers[topic]
	return chs, ok
}

func (s *EventBus) Publish(ctx context.Context, topic string, msg cloner) error {
	subs, ok := s.GetConsumer(topic)
	if !ok {
		return fmt.Errorf("topic %s need subscribers", topic)
	}
	return subs.pub(ctx, msg)
}
