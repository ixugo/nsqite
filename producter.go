package nsqite

type Producter struct {
	queue chan *Message
}

func NewProducter(queueSize int) *Producter {
	if queueSize <= 0 {
		queueSize = 100
	}
	return &Producter{
		queue: make(chan *Message, queueSize),
	}
}

func (p *Producter) publish(topic string, msg *Message) error {
	scheduler.Publish(topic, msg)
	return nil
}

func (p *Producter) Publish(topic string, msg []byte) error {
	p.publish(topic, &Message{
		Body:     msg,
		Delegate: &MDelegate{},
	})
	return nil
}

func (p *Producter) Stop() {
	close(p.queue)
}
