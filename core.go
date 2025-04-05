package nsqite

import "sync"

var scheduler = NewScheduler()

type Scheduler struct {
	consumers map[string]map[string]*Consumer
	m         sync.RWMutex
}

func NewScheduler() *Scheduler {
	return &Scheduler{
		consumers: make(map[string]map[string]*Consumer),
	}
}

func (s *Scheduler) AddConsumer(c *Consumer) {
	s.m.Lock()
	defer s.m.Unlock()

	chs := s.consumers[c.topic]
	if chs == nil {
		chs = make(map[string]*Consumer)
		s.consumers[c.topic] = chs
	}
	chs[c.channel] = c
}

func (s *Scheduler) DelConsumer(c *Consumer) {
	s.m.Lock()
	defer s.m.Unlock()
	delete(s.consumers[c.topic], c.channel)
}

func (s *Scheduler) Publish(topic string, msg *Message) {
	s.m.RLock()
	defer s.m.RUnlock()

	consumers, ok := s.consumers[topic]
	if !ok {
		return
	}

	for _, c := range consumers {
		c.incomingMessages <- msg
	}
}
