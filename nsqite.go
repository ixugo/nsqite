package nsqite

import (
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
		transactionMQ = New(DB())
	})
	return transactionMQ
}

// NSQite 是消息队列的核心结构
type NSQite struct {
	consumers map[string]map[string]*Consumer
	m         sync.RWMutex

	db   *gorm.DB
	exit chan struct{}
	once sync.Once
}

// New 创建一个新的NSQite实例
func New(db *gorm.DB) *NSQite {
	nsqite := &NSQite{
		db:        db,
		exit:      make(chan struct{}),
		consumers: make(map[string]map[string]*Consumer),
	}
	nsqite.db.AutoMigrate(
		&Message{},
	)
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

func (n *NSQite) consumer(topic string) map[string]*Consumer {
	n.m.RLock()
	consumers, ok := n.consumers[topic]
	n.m.RUnlock()
	if !ok {
		consumers = make(map[string]*Consumer)
		n.consumers[topic] = consumers
	}
	return consumers
}

func (n *NSQite) PublishTx(tx *gorm.DB, topic string, msg *Message) error {
	c := n.consumer(topic)
	chs := make([]string, 0, 8)
	for _, consumer := range c {
		chs = append(chs, consumer.channel)
	}
	msg.Consumers = uint32(len(chs))
	msg.Channels = strings.Join(chs, ",")
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
	for _, c := range consumers {
		if err := c.SendMessage(msg); err != nil {
			return err
		}
	}
	return nil
}

func (n *NSQite) AddConsumer(c *Consumer) {
	n.m.Lock()
	defer n.m.Unlock()

	consumer, ok := n.consumers[c.topic]
	if !ok {
		consumer = make(map[string]*Consumer)
		n.consumers[c.topic] = consumer
	}
	consumer[c.channel] = c
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
			if err := n.db.Table(msg.TableName()).Where("responded >= consumers").Count(&count).Error; err != nil {
				slog.Error("messagePump", "error", err)
				continue
			}
			days := DefaultMaxRetentionDays
			if count > DefaultMaxMessageRows {
				days = 3
			}
			if err := n.db.Table(msg.TableName()).Where("responded >= consumers && timestamp < ?", time.Now().AddDate(0, 0, -days)).Delete(nil).Error; err != nil {
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

			for _, m := range msgs {
				consumers := n.consumer(m.Topic)
				chs := strings.Split(m.RespondedChannels, ",")
				for _, c := range consumers {
					if !slices.Contains(chs, c.channel) {
						c.sendMessage(&m)
					}
				}
			}
			timer.Reset(3 * time.Second)
		}
	}
}

func (n *NSQite) Finish(msg *Message, channel string) error {
	return n.db.Transaction(func(tx *gorm.DB) error {
		var message Message
		if err := tx.Table(msg.TableName()).Where("id=?", msg.ID).Clauses(clause.Locking{Strength: "UPDATE"}).First(&message).Error; err != nil {
			return err
		}

		chs := strings.Split(message.RespondedChannels, ",")
		for _, ch := range chs {
			if ch == channel {
				return nil
			}
		}
		if chs[0] == "" {
			chs = chs[1:]
		}
		message.RespondedChannels = strings.Join(append(chs, channel), ",")
		message.Responded++
		return tx.Table(msg.TableName()).Save(&message).Error
	})
}

func (n *NSQite) DelConsumer(c *Consumer) {
	n.m.Lock()
	defer n.m.Unlock()
	delete(n.consumers[c.topic], c.channel)
}
