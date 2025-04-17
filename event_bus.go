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
	subscribers map[string]map[string]SubscriberInfo
	m           sync.RWMutex
}

func newEventBus() *EventBus {
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

func (s *EventBus) Publish(ctx context.Context, topic string, msg cloner) error {
	s.m.RLock()
	subs, ok := s.subscribers[topic]
	s.m.RUnlock()
	if !ok {
		return fmt.Errorf("topic %s need subscribers", topic)
	}

	var i int
	for _, c := range subs {
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
