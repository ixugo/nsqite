package gorm

import (
	"time"

	"github.com/ixugo/nsqite/storage"
	"gorm.io/gorm"
)

// GormStorage 实现了Storage接口，使用GORM存储
type GormStorage struct {
	db *gorm.DB
}

// NewGormStorage 创建新的GORM存储实例
func NewGormStorage(db *gorm.DB) *GormStorage {
	return &GormStorage{db: db}
}

// Init 初始化GORM存储
func (s *GormStorage) Init(config interface{}) error {
	// 自动迁移表结构
	return s.db.AutoMigrate(
		&storage.Topic{},
		&storage.Channel{},
		&storage.Message{},
		&storage.ChannelMessage{},
	)
}

// Close 关闭GORM存储
func (s *GormStorage) Close() error {
	// GORM不需要特殊关闭
	return nil
}

// AddTopic 添加主题
func (s *GormStorage) AddTopic(topic *storage.Topic) error {
	return s.db.Create(topic).Error
}

// GetTopic 获取主题
func (s *GormStorage) GetTopic(name string) (*storage.Topic, error) {
	var topic storage.Topic
	err := s.db.Where("name = ?", name).First(&topic).Error
	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	return &topic, err
}

// EditTopic 编辑主题
func (s *GormStorage) EditTopic(topic *storage.Topic) error {
	topic.UpdatedAt = time.Now()
	return s.db.Save(topic).Error
}

// DelTopic 删除主题
func (s *GormStorage) DelTopic(name string) error {
	return s.db.Where("name = ?", name).Delete(&storage.Topic{}).Error
}

// AddChannel 添加通道
func (s *GormStorage) AddChannel(channel *storage.Channel) error {
	return s.db.Create(channel).Error
}

// GetChannel 获取通道
func (s *GormStorage) GetChannel(topicName, channelName string) (*storage.Channel, error) {
	var channel storage.Channel
	err := s.db.Joins("JOIN topics ON topics.id = channels.topic_id").
		Where("topics.name = ? AND channels.name = ?", topicName, channelName).
		First(&channel).Error
	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	return &channel, err
}

// FindChannels 查找主题下的所有通道
func (s *GormStorage) FindChannels(topicName string) ([]*storage.Channel, error) {
	var channels []*storage.Channel
	err := s.db.Joins("JOIN topics ON topics.id = channels.topic_id").
		Where("topics.name = ?", topicName).
		Find(&channels).Error
	return channels, err
}

// EditChannel 编辑通道
func (s *GormStorage) EditChannel(channel *storage.Channel) error {
	channel.UpdatedAt = time.Now()
	return s.db.Save(channel).Error
}

// DelChannel 删除通道
func (s *GormStorage) DelChannel(topicName, channelName string) error {
	return s.db.Joins("JOIN topics ON topics.id = channels.topic_id").
		Where("topics.name = ? AND channels.name = ?", topicName, channelName).
		Delete(&storage.Channel{}).Error
}

// SaveMessage 保存消息
func (s *GormStorage) SaveMessage(message *storage.Message) error {
	return s.db.Create(message).Error
}

// GetMessage 获取消息
func (s *GormStorage) GetMessage(id string) (*storage.Message, error) {
	var message storage.Message
	err := s.db.Where("id = ?", id).First(&message).Error
	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	return &message, err
}

// FindMessages 查找主题下的消息
func (s *GormStorage) FindMessages(topicName string, limit, offset int) ([]*storage.Message, error) {
	var messages []*storage.Message
	err := s.db.Joins("JOIN topics ON topics.id = messages.topic_id").
		Where("topics.name = ?", topicName).
		Order("messages.created_at DESC").
		Limit(limit).
		Offset(offset).
		Find(&messages).Error
	return messages, err
}

// SaveChannelMessage 保存通道消息
func (s *GormStorage) SaveChannelMessage(channelMessage *storage.ChannelMessage) error {
	return s.db.Create(channelMessage).Error
}

// GetChannelMessage 获取通道消息
func (s *GormStorage) GetChannelMessage(channelID int, messageID string) (*storage.ChannelMessage, error) {
	var channelMessage storage.ChannelMessage
	err := s.db.Where("channel_id = ? AND message_id = ?", channelID, messageID).
		First(&channelMessage).Error
	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	return &channelMessage, err
}

// FindChannelMessages 查找通道消息
func (s *GormStorage) FindChannelMessages(channelID int, delivered, finished, requeued int, limit, offset int) ([]*storage.ChannelMessage, error) {
	query := s.db.Where("channel_id = ?", channelID)

	if delivered != -1 {
		query = query.Where("delivered = ?", delivered == 1)
	}
	if finished != -1 {
		query = query.Where("finished = ?", finished == 1)
	}
	if requeued != -1 {
		query = query.Where("requeued = ?", requeued == 1)
	}

	var channelMessages []*storage.ChannelMessage
	err := query.Order("created_at DESC").
		Limit(limit).
		Offset(offset).
		Find(&channelMessages).Error
	return channelMessages, err
}

// EditChannelMessage 编辑通道消息
func (s *GormStorage) EditChannelMessage(channelMessage *storage.ChannelMessage) error {
	return s.db.Save(channelMessage).Error
}

// Subscribe 订阅消息
func (s *GormStorage) Subscribe(topicName string, channelName string) (<-chan *storage.Message, error) {
	msgChan := make(chan *storage.Message)
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for range ticker.C {
			var messages []*storage.Message
			err := s.db.Joins("JOIN topics ON topics.id = messages.topic_id").
				Joins("JOIN channels ON channels.topic_id = topics.id").
				Joins("LEFT JOIN channel_messages ON channel_messages.channel_id = channels.id AND channel_messages.message_id = messages.id").
				Where("topics.name = ? AND channels.name = ?", topicName, channelName).
				Where("channel_messages.id IS NULL OR (channel_messages.delivered = 0 AND channel_messages.finished = 0 AND channel_messages.requeued = 0)").
				Order("messages.created_at ASC").
				Limit(100).
				Find(&messages).Error

			if err == nil {
				for _, msg := range messages {
					msgChan <- msg
				}
			}
		}
	}()
	return msgChan, nil
}

// Unsubscribe 取消订阅
func (s *GormStorage) Unsubscribe(topicName string, channelName string) error {
	// GORM实现不需要特殊处理
	return nil
}

// BeginTx 开始事务
func (s *GormStorage) BeginTx() (storage.Transaction, error) {
	tx := s.db.Begin()
	if tx.Error != nil {
		return nil, tx.Error
	}
	return &GormTransaction{tx: tx}, nil
}

// GormTransaction 实现了Transaction接口
type GormTransaction struct {
	tx *gorm.DB
}

// Commit 提交事务
func (t *GormTransaction) Commit() error {
	return t.tx.Commit().Error
}

// Rollback 回滚事务
func (t *GormTransaction) Rollback() error {
	return t.tx.Rollback().Error
}
