package main

import (
	"fmt"

	"github.com/ixugo/nsqite"
)

type AlertType string

type Handler2 struct{}

// HandleMessage implements nsqite.EventHandler.
func (h Handler2) HandleMessage(message *nsqite.EventMessage[AlertType]) error {
	fmt.Println("user >>> ", message.Body)
	return nil
}

func InitUserSubscriber(topic string) {
	sub2 := nsqite.NewSubscriber[AlertType](topic, "ch2_db")
	sub2.AddConcurrentHandlers(new(Handler2), 1)
}
