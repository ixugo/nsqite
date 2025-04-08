package nsqite

type Publisher[T any] struct {
	queue chan *EventMessage[T]
}

func NewPublisher[T any]() *Publisher[T] {
	return &Publisher[T]{
		queue: make(chan *EventMessage[T]),
	}
}

func (p *Publisher[T]) publish(topic string, msg *EventMessage[T]) error {
	eventBus.Publish(topic, msg)
	return nil
}

func (p *Publisher[T]) Publish(topic string, msg T) error {
	p.publish(topic, &EventMessage[T]{
		Body:     msg,
		Delegate: &EventMDelegate[T]{},
	})
	return nil
}

func (p *Publisher[T]) Stop() {
	close(p.queue)
}
