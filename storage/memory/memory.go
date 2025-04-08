package memory

import (
	"sync"

	"github.com/ixugo/nsqite/storage"
)

// MemoryStorage 内存存储实现
type MemoryStorage struct {
	topics          map[string]*storage.Topic
	channels        map[string]*storage.Channel
	messages        map[string]*storage.Message
	channelMessages map[string]*storage.ChannelMessage
	mu              sync.RWMutex
}

// NewMemoryStorage 创建新的内存存储实例
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		topics:          make(map[string]*storage.Topic),
		channels:        make(map[string]*storage.Channel),
		messages:        make(map[string]*storage.Message),
		channelMessages: make(map[string]*storage.ChannelMessage),
	}
}

// Init 初始化存储
func (s *MemoryStorage) Init(config interface{}) error {
	return nil
}

// Close 关闭存储
func (s *MemoryStorage) Close() error {
	return nil
}

// AddTopic 添加主题
func (s *MemoryStorage) AddTopic(topic *storage.Topic) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topics[topic.Name] = topic
	return nil
}

// GetTopic 获取主题
func (s *MemoryStorage) GetTopic(name string) (*storage.Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.topics[name], nil
}

// EditTopic 编辑主题
func (s *MemoryStorage) EditTopic(topic *storage.Topic) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topics[topic.Name] = topic
	return nil
}

// DelTopic 删除主题
func (s *MemoryStorage) DelTopic(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.topics, name)
	return nil
}

// AddChannel 添加通道
func (s *MemoryStorage) AddChannel(channel *storage.Channel) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.channels[channel.Name] = channel
	return nil
}

// GetChannel 获取通道
func (s *MemoryStorage) GetChannel(topicName, channelName string) (*storage.Channel, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.channels[channelName], nil
}

// FindChannels 查找所有通道
func (s *MemoryStorage) FindChannels(topicName string) ([]*storage.Channel, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 检查 topic 是否存在
	_, exists := s.topics[topicName]
	if !exists {
		return []*storage.Channel{}, nil
	}

	channels := make([]*storage.Channel, 0)
	for _, channel := range s.channels {
		if channel.Topic == topicName {
			channels = append(channels, channel)
		}
	}
	return channels, nil
}

// EditChannel 编辑通道
func (s *MemoryStorage) EditChannel(channel *storage.Channel) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.channels[channel.Name] = channel
	return nil
}

// DelChannel 删除通道
func (s *MemoryStorage) DelChannel(topicName, channelName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.channels, channelName)
	return nil
}

// SaveMessage 保存消息
func (s *MemoryStorage) SaveMessage(message *storage.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages[message.ID] = message
	return nil
}

// GetMessage 获取消息
func (s *MemoryStorage) GetMessage(id string) (*storage.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.messages[id], nil
}

// FindMessages 查找消息
func (s *MemoryStorage) FindMessages(topicName string, limit, offset int) ([]*storage.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 检查 topic 是否存在
	_, exists := s.topics[topicName]
	if !exists {
		return []*storage.Message{}, nil
	}

	messages := make([]*storage.Message, 0)
	for _, message := range s.messages {
		if message.Topic == topicName {
			messages = append(messages, message)
		}
	}
	return messages, nil
}

// SaveChannelMessage 保存通道消息
func (s *MemoryStorage) SaveChannelMessage(channelMessage *storage.ChannelMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.channelMessages[channelMessage.MessageID] = channelMessage
	return nil
}

// GetChannelMessage 获取通道消息
func (s *MemoryStorage) GetChannelMessage(channelID int, messageID string) (*storage.ChannelMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.channelMessages[messageID], nil
}

// FindChannelMessages 查找通道消息
func (s *MemoryStorage) FindChannelMessages(channelID int, delivered, finished, requeued int, limit, offset int) ([]*storage.ChannelMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	messages := make([]*storage.ChannelMessage, 0)
	for _, message := range s.channelMessages {
		if message.ChannelID == int64(channelID) {
			messages = append(messages, message)
		}
	}
	return messages, nil
}

// EditChannelMessage 编辑通道消息
func (s *MemoryStorage) EditChannelMessage(channelMessage *storage.ChannelMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.channelMessages[channelMessage.MessageID] = channelMessage
	return nil
}

// Subscribe 订阅消息
func (s *MemoryStorage) Subscribe(topicName string, channelName string) (<-chan *storage.Message, error) {
	msgChan := make(chan *storage.Message, 100)
	go func() {
		defer close(msgChan)

		s.mu.RLock()
		_, exists := s.topics[topicName]
		if !exists {
			s.mu.RUnlock()
			return
		}
		s.mu.RUnlock()

		for _, message := range s.messages {
			if message.Topic == topicName {
				msgChan <- message
			}
		}
	}()
	return msgChan, nil
}

// Unsubscribe 取消订阅
func (s *MemoryStorage) Unsubscribe(topicName string, channelName string) error {
	return nil
}

// BeginTx 开始事务
func (s *MemoryStorage) BeginTx() (storage.Transaction, error) {
	return &MemoryTransaction{}, nil
}

// MemoryTransaction 内存事务实现
type MemoryTransaction struct{}

// Commit 提交事务
func (t *MemoryTransaction) Commit() error {
	return nil
}

// Rollback 回滚事务
func (t *MemoryTransaction) Rollback() error {
	return nil
}
