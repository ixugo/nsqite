package main

import (
	"fmt"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/ixugo/nsqite"
	"gorm.io/gorm"
)

func main() {
	db, err := gorm.Open(sqlite.Open("test.db"))
	if err != nil {
		panic(err)
	}

	const topic = "alert"

	nsqite.SetGorm(db)

	p := nsqite.NewProducer()
	c := nsqite.NewConsumer(topic, "ch", nsqite.WithConsumerMaxAttempts(10))
	c2 := nsqite.NewConsumer(topic, "ch2", nsqite.WithConsumerMaxAttempts(3))

	var i int

	c.AddConcurrentHandlers(nsqite.ConsumerHandlerFunc(func(msg *nsqite.Message) error {
		i++
		fmt.Printf("c1 %d websocket >>> %s \n\n", i, string(msg.Body))
		if i >= 2 {
			fmt.Println("完成")
			return nil
		}
		return fmt.Errorf("err")
	}), 1)

	c2.AddConcurrentHandlers(nsqite.ConsumerHandlerFunc(func(msg *nsqite.Message) error {
		fmt.Printf("c2 websocket >>> %s \n\n", string(msg.Body))
		return nil
	}), 1)

	db.Transaction(func(tx *gorm.DB) error {
		p.PublishTx(tx, topic, []byte("1111111111"))
		return nil
	})

	time.Sleep(10 * time.Second)
	fmt.Println("end")
}
