package nsqite

type Storer interface {
	AutoMigrate() error
	AddMessage(msg *Message) error
	FindMessages(topic string)
}
