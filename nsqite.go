package nsqite

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// DefaultMaxRetentionDays 定义消息的默认最大保留天数为7天
// 超过此天数的消息将被自动清理
const DefaultMaxRetentionDays = 7

// DefaultMaxMessageRows 定义消息表的默认最大行数
// 当消息表行数超过此值时，将仅保留最近3天的消息
const DefaultMaxMessageRows = 10000

var transactionMQ *NSQite

var once sync.Once

func TransactionMQ() *NSQite {
	once.Do(func() {
		transactionMQ = newNSQite(gormDB())
	})
	return transactionMQ
}

// NSQite 是消息队列的核心结构
type NSQite struct {
	consumers map[string]*consumerMap
	m         sync.RWMutex

	db   *gorm.DB
	exit chan struct{}
	once sync.Once
}

type consumerMap struct {
	m    sync.RWMutex
	data map[string]*Consumer
}

func (c *consumerMap) add(channel string, consumer *Consumer) {
	c.m.Lock()
	defer c.m.Unlock()
	c.data[channel] = consumer
}

func (c *consumerMap) del(channel string) {
	c.m.Lock()
	defer c.m.Unlock()
	delete(c.data, channel)
}

func (c *consumerMap) Len() int {
	c.m.RLock()
	defer c.m.RUnlock()
	return len(c.data)
}

func (c *consumerMap) Channels() (string, uint32) {
	c.m.RLock()
	defer c.m.RUnlock()
	chs := make([]string, 0, 8)
	for ch := range c.data {
		chs = append(chs, ch)
	}
	return strings.Join(chs, ","), uint32(len(chs))
}

func (c *consumerMap) pub(ctx context.Context, msg *Message) error {
	c.m.RLock()
	defer c.m.RUnlock()

	var i int
	for _, c := range c.data {
		i++
		m := msg
		if i > 1 {
			mm := *msg
			m = &mm
		}
		if err := c.SendMessage(m); err != nil {
			return err
		}
	}
	return nil
}

func (c *consumerMap) pumpPub(chs []string, msg Message) {
	c.m.RLock()
	defer c.m.RUnlock()

	for _, c := range c.data {
		if !slices.Contains(chs, c.channel) {
			// TODO: 若发送阻塞，会导致整个消息泵阻塞
			// 可以增加延迟队列，将发送失败的消息，放到延迟队列里重试，优先处理未阻塞的消费者
			_ = c.sendMessage(msg)
		}
	}
}

// newNSQite 创建一个新的NSQite实例
func newNSQite(db *gorm.DB) *NSQite {
	nsqite := &NSQite{
		db:        db,
		exit:      make(chan struct{}),
		consumers: make(map[string]*consumerMap),
	}
	if err := nsqite.db.AutoMigrate(
		&Message{},
	); err != nil {
		panic(err)
	}
	// 启动消息泵
	go nsqite.messagePump()
	return nsqite
}

// Close 关闭NSQite实例
func (n *NSQite) Close() error {
	n.once.Do(func() {
		close(n.exit)
	})
	return nil
}

func (n *NSQite) consumer(topic string) *consumerMap {
	n.m.RLock()
	consumers, ok := n.consumers[topic]
	n.m.RUnlock()
	if ok {
		return consumers
	}

	n.m.Lock()
	defer n.m.Unlock()
	consumers, ok = n.consumers[topic]
	if ok {
		return consumers
	}

	consumers = &consumerMap{
		data: make(map[string]*Consumer),
	}
	n.consumers[topic] = consumers
	return consumers
}

func (n *NSQite) PublishTx(tx *gorm.DB, topic string, msg *Message) error {
	c := n.consumer(topic)
	msg.Channels, msg.Consumers = c.Channels()
	if err := tx.Create(msg).Error; err != nil {
		return err
	}
	return nil
}

// Publish 发布消息到Topic
func (n *NSQite) Publish(topic string, msg *Message) error {
	n.m.RLock()
	consumers, ok := n.consumers[topic]
	n.m.RUnlock()
	if !ok {
		return fmt.Errorf("topic %s need consumers", topic)
	}

	if err := n.PublishTx(n.db, topic, msg); err != nil {
		return err
	}

	return consumers.pub(context.Background(), msg)
}

func (n *NSQite) AddConsumer(c *Consumer) {
	n.m.Lock()
	defer n.m.Unlock()

	consumer, ok := n.consumers[c.topic]
	if !ok {
		consumer = &consumerMap{
			data: make(map[string]*Consumer),
		}
		n.consumers[c.topic] = consumer
	}
	consumer.add(c.channel, c)
}

// GetTimeUntilMidnight 返回距离下一个凌晨12点的时间间隔
func GetTimeUntilMidnight() time.Duration {
	now := time.Now()
	tomorrow := now.Add(24 * time.Hour)
	midnight := time.Date(tomorrow.Year(), tomorrow.Month(), tomorrow.Day(), 0, 0, 0, 0, tomorrow.Location())
	return midnight.Sub(now)
}

// messagePump 消息泵，处理消息超时和重试
func (n *NSQite) messagePump() {
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()

	cleanUpTimer := time.NewTimer(GetTimeUntilMidnight())
	defer cleanUpTimer.Stop()

	var id int
	var msg Message
	var count int64
	msgs := make([]Message, 0, 24)
	for {
		select {
		case <-n.exit:
			return
		case <-cleanUpTimer.C:
			cleanUpTimer.Reset(GetTimeUntilMidnight())
			now := time.Now()

			if err := n.db.Table(msg.TableName()).Where("timestamp < ?", now.AddDate(0, 0, -15)).Delete(nil).Error; err != nil {
				slog.Error("messagePump", "error", err)
			}

			if err := n.db.Table(msg.TableName()).Where("responded >= consumers").Count(&count).Error; err != nil {
				slog.Error("messagePump", "error", err)
				continue
			}
			days := DefaultMaxRetentionDays
			if count > DefaultMaxMessageRows {
				days = 3
			}
			if err := n.db.Table(msg.TableName()).Where("responded >= consumers AND timestamp < ?", now.AddDate(0, 0, -days)).Delete(nil).Error; err != nil {
				slog.Error("messagePump", "error", err)
				continue
			}
		case <-timer.C:
			timer.Reset(10 * time.Second)
			msgs = msgs[:0]
			if err := n.db.Table(msg.TableName()).Order("id ASC").Where("id > ? AND responded<consumers", id).Find(&msgs).Error; err != nil {
				slog.Error("messagePump", "error", err)
				continue
			}
			if len(msgs) > 0 {
				id = msgs[len(msgs)-1].ID
			}

			for _, msg := range msgs {
				consumers := n.consumer(msg.Topic)
				chs := strings.Split(msg.RespondedChannels, ",")

				consumers.pumpPub(chs, msg)
			}
			timer.Reset(3 * time.Second)
		}
	}
}

func (n *NSQite) Finish(msg *Message, channel string) error {
	return n.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Table(msg.TableName()).Clauses(clause.Locking{Strength: "UPDATE"}).First(msg).Error; err != nil {
			return err
		}

		channels := make([]string, 0, 8)

		chs := strings.Split(msg.RespondedChannels, ",")
		for _, ch := range chs {
			if ch == channel {
				return nil
			}
			if ch != "" {
				channels = append(channels, ch)
			}
		}
		msg.RespondedChannels = strings.Join(append(channels, channel), ",")
		msg.Responded++
		return tx.Table(msg.TableName()).Where("id=?", msg.ID).Select("responded_channels", "responded").Updates(msg).Error
	})
}

func (n *NSQite) DelConsumer(topic, channel string) {
	n.m.Lock()
	defer n.m.Unlock()

	chs, ok := n.consumers[topic]
	if !ok {
		return
	}
	chs.del(channel)
	if chs.Len() == 0 {
		delete(n.consumers, topic)
	}
}
