# 基于持久化接口的简单版NSQite设计

## 1. 系统概述

本文档描述了一个简化版NSQite消息队列系统设计。该系统保留了NSQ的核心功能，同时简化了实现，使用可插拔的持久化接口作为消息存储和状态管理的后端。持久化接口可以有多种实现，如SQLite、MySQL、PostgreSQL或GORM等。

## 2. 核心组件

### 2.1 主要组件

- **NSQite**: 主服务器组件，负责消息的接收、存储和分发
- **Topic**: 消息的主题，每个主题可以有多个Channel
- **Channel**: 消息的通道，每个Channel可以有多个消费者
- **Message**: 消息实体，包含消息ID、内容、时间戳等信息
- **Storage**: 持久化接口，用于存储消息和状态，可以有多种实现

- **Consumer**: 消费者组件，负责从Channel中获取消息并消费
- **Producer**: 生产者组件，负责生产消息并发送到Channel

### 2.2 数据模型

#### 持久化接口定义

```go
// Storage 定义了持久化接口
type Storage interface {
    // 初始化存储
    Init(config interface{}) error

    // 关闭存储连接
    Close() error

    // Channel相关操作
    AddChannel(channel *Channel) error
    GetChannel(topicName, channelName string) (*Channel, error)
    FindChannels(topicName string) ([]*Channel, error)
    EditChannel(channel *Channel) error
    DelChannel(topicName, channelName string) error

    // Message相关操作
    SaveMessage(message *Message) error
    GetMessage(id string) (*Message, error)
    FindMessages(topicName string, limit, offset int) ([]*Message, error)

    // ChannelMessage相关操作
    SaveChannelMessage(channelMessage *ChannelMessage) error
    GetChannelMessage(channelID int, messageID string) (*ChannelMessage, error)
    FindChannelMessages(channelID int, delivered, finished, requeued int, limit, offset int) ([]*ChannelMessage, error)
    EditChannelMessage(channelMessage *ChannelMessage) error

    // 消息订阅相关操作
    Subscribe(topicName string, channelName string) (<-chan *Message, error)
    Unsubscribe(topicName string, channelName string) error

    // 事务支持
    BeginTx() (Transaction, error)
}

// Transaction 定义了事务接口
type Transaction interface {
    Commit() error
    Rollback() error
}
```

#### 数据实体定义

1. **Topic实体**
   ```go
   type Topic struct {
       ID            int64     `json:"id"`
       Name          string    `json:"name"`
       CreatedAt     time.Time `json:"created_at"`
       UpdatedAt     time.Time `json:"updated_at"`
       MessageCount  int64     `json:"message_count"`
       MessageBytes  int64     `json:"message_bytes"`
       Paused        bool      `json:"paused"`
   }
   ```

2. **Channel实体**
   ```go
   type Channel struct {
       ID            int64     `json:"id"`
       TopicID       int64     `json:"topic_id"`
       Name          string    `json:"name"`
       CreatedAt     time.Time `json:"created_at"`
       UpdatedAt     time.Time `json:"updated_at"`
       MessageCount  int64     `json:"message_count"`
       Paused        bool      `json:"paused"`
   }
   ```

3. **Message实体**
   ```go
   type Message struct {
       ID        string    `json:"id"`
       TopicID   int64     `json:"topic_id"`
       Body      []byte    `json:"body"`
       CreatedAt time.Time `json:"created_at"`
       Deferred  int64     `json:"deferred"` // 延迟投递时间(纳秒)
   }
   ```

4. **ChannelMessage实体**
   ```go
   type ChannelMessage struct {
       ID          int64     `json:"id"`
       ChannelID   int64     `json:"channel_id"`
       MessageID   string    `json:"message_id"`
       CreatedAt   time.Time `json:"created_at"`
       Delivered   bool      `json:"delivered"`
       DeliveredAt time.Time `json:"delivered_at,omitempty"`
       Finished    bool      `json:"finished"`
       FinishedAt  time.Time `json:"finished_at,omitempty"`
       Requeued    bool      `json:"requeued"`
       RequeuedAt  time.Time `json:"requeued_at,omitempty"`
       Attempts    int       `json:"attempts"`
   }
   ```

## 3. 核心功能实现

### 3.1 消息发布流程

1. 客户端连接到NSQite并发送PUB命令
2. NSQite验证Topic是否存在，不存在则创建
3. 生成消息ID，将消息内容写入存储
4. 更新topic的message_count和message_bytes
5. 为每个channel创建channel_messages记录
6. 返回OK响应给客户端

### 3.2 消息订阅流程

1. 客户端连接到NSQite并发送SUB命令
2. NSQite验证Topic和Channel是否存在，不存在则创建
3. 将客户端信息写入存储
4. 启动消息泵，从存储中获取未投递的消息
5. 将消息发送给客户端，更新channel_messages的delivered状态
6. 客户端处理完成后发送FIN命令，更新channel_messages的finished状态

### 3.3 消息重试机制

1. 客户端发送REQ命令请求重试消息
2. NSQite更新channel_messages的requeued状态
3. 如果指定了延迟时间，更新deferred字段
4. 消息重新进入投递队列

### 3.4 消息超时处理

1. 定期检查channel_messages中delivered=true且finished=false的记录
2. 如果消息超过超时时间，自动重新入队
3. 更新attempts计数和requeued状态

## 4. 系统启动和恢复

### 4.1 系统启动

1. 初始化持久化接口
2. 被动加载 topics 和 channels（按需创建）
3. 启动消息泵和超时检查协程

### 4.2 状态恢复

1. 从存储中读取所有未完成的消息状态
2. 重新建立客户端连接

### 4.3 被动加载机制

1. Topics 和 Channels 采用被动加载机制，只在需要时才会被创建和加载
2. 这种机制可以节省系统启动时的资源消耗，特别是在 topics 和 channels 数量较多时
3. 当发布者或订阅者首次访问某个 topic 或 channel 时，系统会自动创建并加载它
4. 使用双重检查锁定模式确保线程安全

### 4.4 Topic 懒加载机制

1. Topic 不再进行数据库级别的持久化，而是采用内存中的懒加载方式
2. 当第一次访问某个 Topic 时，系统会在内存中创建它，并分配一个基于时间戳的唯一 ID
3. 这种机制可以进一步减少系统启动时的资源消耗，并简化系统设计
4. Topic 的状态（如消息计数、字节数等）仅在内存中维护，系统重启后会重置
5. 只有 Channel 和 Message 相关的数据会被持久化到数据库中

## 5. 性能优化

### 5.1 内存缓存

1. 使用内存缓存热点消息
2. 实现LRU缓存策略
3. 定期将缓存数据同步到存储

### 5.2 批量操作

1. 批量插入消息
2. 批量更新消息状态
3. 使用事务提高写入性能

### 5.3 索引优化

1. 为常用查询创建索引
2. 定期维护索引
3. 使用覆盖索引减少IO

## 6. 扩展功能

### 6.1 消息过滤

1. 支持基于消息内容的过滤
2. 实现消息标签系统
3. 支持消息优先级

### 6.2 消息压缩

1. 支持消息内容压缩
2. 实现压缩算法选择
3. 自动检测压缩率

### 6.3 监控和统计

1. 实现基本的统计信息收集
2. 提供HTTP API查询统计信息
3. 支持导出统计数据

## 7. 实现计划

### 7.1 第一阶段：核心功能

1. 实现基本的Topic和Channel管理
2. 实现消息发布和订阅
3. 实现基本的消息重试机制

### 7.2 第二阶段：持久化和恢复

1. 实现持久化接口
2. 实现系统启动和恢复
3. 实现消息超时处理

### 7.3 第三阶段：性能优化

1. 实现内存缓存
2. 优化批量操作
3. 优化索引

### 7.4 第四阶段：扩展功能

1. 实现消息过滤
2. 实现消息压缩
3. 实现监控和统计

## 8. 与NSQ的对比

### 8.1 保留的功能

1. Topic和Channel的概念
2. 消息发布和订阅模式
3. 消息重试和超时机制
4. 消息持久化

### 8.2 简化的部分

1. 使用可插拔的持久化接口替代自定义的磁盘队列
2. 简化了消息分发机制
3. 移除了复杂的拓扑感知功能
4. 移除了lookupd服务发现

### 8.3 改进的部分

1. 更简单的部署和维护
2. 更容易理解和调试
3. 更适合小规模应用场景
4. 更容易扩展和定制
5. 支持多种数据库后端

## 9. 持久化接口实现示例

### 9.1 存储接口定义

```go
// DBConnector 定义了数据库连接接口
type DBConnector interface {
    Connect(config interface{}) error
    Close() error
    Begin() (Transaction, error)
    Exec(query string, args ...interface{}) error
    Query(query string, args ...interface{}) (Rows, error)
    QueryRow(query string, args ...interface{}) Row
}

// Rows 定义了查询结果集接口
type Rows interface {
    Next() bool
    Scan(dest ...interface{}) error
    Close() error
}

// Row 定义了单行查询结果接口
type Row interface {
    Scan(dest ...interface{}) error
}

// StreamConnector 定义了流式存储接口
type StreamConnector interface {
    Connect(config interface{}) error
    Close() error
    Publish(stream string, message interface{}) error
    Subscribe(stream string, group string, consumer string) (<-chan interface{}, error)
    Acknowledge(stream string, group string, messageID string) error
}

// NotifyConnector 定义了通知存储接口
type NotifyConnector interface {
    Connect(config interface{}) error
    Close() error
    Listen(channel string) error
    Notify(channel string, payload interface{}) error
    Subscribe() (<-chan interface{}, error)
}
```

### 9.2 Redis Stream 实现

```go
// RedisStreamStorage 实现了Storage接口
type RedisStreamStorage struct {
    stream StreamConnector
}

// 实现Storage接口的所有方法
func (s *RedisStreamStorage) Init(config interface{}) error {
    return s.stream.Connect(config)
}

// Subscribe 实现基于Stream的订阅
func (s *RedisStreamStorage) Subscribe(topicName string, channelName string) (<-chan *Message, error) {
    return s.stream.Subscribe(topicName, channelName, "consumer-1")
}

// SaveMessage 实现基于Stream的消息存储
func (s *RedisStreamStorage) SaveMessage(message *Message) error {
    return s.stream.Publish(message.TopicID, message)
}
```

### 9.3 PostgreSQL Notify 实现

```go
// PostgresNotifyStorage 实现了Storage接口
type PostgresNotifyStorage struct {
    db      DBConnector
    notify  NotifyConnector
}

// 实现Storage接口的所有方法
func (s *PostgresNotifyStorage) Init(config interface{}) error {
    if err := s.db.Connect(config); err != nil {
        return err
    }
    return s.notify.Connect(config)
}

// Subscribe 实现基于Notify的订阅
func (s *PostgresNotifyStorage) Subscribe(topicName string, channelName string) (<-chan *Message, error) {
    if err := s.notify.Listen(channelName); err != nil {
        return nil, err
    }
    return s.notify.Subscribe()
}

// SaveMessage 实现基于Notify的消息存储
func (s *PostgresNotifyStorage) SaveMessage(message *Message) error {
    if err := s.db.Exec("INSERT INTO messages (id, topic_id, body, created_at) VALUES (?, ?, ?, ?)",
        message.ID, message.TopicID, message.Body, message.CreatedAt); err != nil {
        return err
    }
    return s.notify.Notify(message.TopicID, message)
}
```

### 9.4 SQLite 轮询实现

```go
// SQLitePollingStorage 实现了Storage接口
type SQLitePollingStorage struct {
    db DBConnector
}

// 实现Storage接口的所有方法
func (s *SQLitePollingStorage) Init(config interface{}) error {
    return s.db.Connect(config)
}

// Subscribe 实现基于轮询的订阅
func (s *SQLitePollingStorage) Subscribe(topicName string, channelName string) (<-chan *Message, error) {
    msgChan := make(chan *Message)
    go func() {
        ticker := time.NewTicker(100 * time.Millisecond)
        defer ticker.Stop()

        for range ticker.C {
            rows, err := s.db.Query(
                "SELECT id, topic_id, body, created_at FROM messages WHERE topic_id = ? AND id NOT IN (SELECT message_id FROM channel_messages WHERE channel_id = ?)",
                topicName, channelName)
            if err != nil {
                continue
            }

            for rows.Next() {
                var msg Message
                if err := rows.Scan(&msg.ID, &msg.TopicID, &msg.Body, &msg.CreatedAt); err != nil {
                    continue
                }
                msgChan <- &msg
            }
            rows.Close()
        }
    }()
    return msgChan, nil
}

// SaveMessage 实现基于SQLite的消息存储
func (s *SQLitePollingStorage) SaveMessage(message *Message) error {
    return s.db.Exec("INSERT INTO messages (id, topic_id, body, created_at) VALUES (?, ?, ?, ?)",
        message.ID, message.TopicID, message.Body, message.CreatedAt)
}
```

## 10. 持久化后端特性对比

### 10.1 Redis Stream 特性

1. 优点：
   - 高性能的消息存储和分发
   - 内置的消息持久化
   - 支持消息分组和消费者组
   - 支持消息确认和重试机制

2. 缺点：
   - 内存占用较大
   - 数据持久化需要额外配置
   - 不适合存储大量历史消息

### 10.2 PostgreSQL Notify 特性

1. 优点：
   - 强一致性保证
   - 支持事务和ACID
   - 适合存储大量历史消息
   - 支持复杂的查询和过滤

2. 缺点：
   - 消息分发性能相对较低
   - 需要额外的连接管理
   - 消息大小有限制

### 10.3 SQLite 轮询特性

1. 优点：
   - 轻量级，无需额外服务
   - 适合嵌入式应用
   - 支持事务和ACID
   - 部署简单

2. 缺点：
   - 轮询机制可能造成延迟
   - 并发性能有限
   - 不适合高吞吐量场景

### 10.4 GORM 实现特性

1. 优点：
   - 支持多种数据库后端
   - 提供统一的ORM接口
   - 自动迁移表结构
   - 支持事务和关联查询

2. 缺点：
   - 性能开销相对较大
   - 复杂查询可能不够灵活
   - 需要额外的依赖

## 11. 实现细节

### 11.1 GORM 存储实现

我们使用 GORM 作为主要的存储实现，它提供了统一的 ORM 接口，支持多种数据库后端。对于 SQLite，我们使用 `github.com/glebarez/sqlite` 作为驱动，这是一个纯 Go 实现的 SQLite 驱动，不需要 CGO 支持，更适合跨平台部署。

```go
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
```

### 11.2 内存存储实现

为了测试和开发，我们还提供了一个内存存储实现，它使用 Go 的内置 map 和 channel 来模拟持久化存储。这个实现适合单元测试和快速原型开发。

```go
// MemoryStorage 实现了Storage接口，使用内存存储
type MemoryStorage struct {
    topics         map[string]*storage.Topic
    channels       map[string]*storage.Channel
    messages       map[string]*storage.Message
    channelMessages map[string]*storage.ChannelMessage
    mu             sync.RWMutex
}

// NewMemoryStorage 创建新的内存存储实例
func NewMemoryStorage() *MemoryStorage {
    return &MemoryStorage{
        topics:         make(map[string]*storage.Topic),
        channels:       make(map[string]*storage.Channel),
        messages:       make(map[string]*storage.Message),
        channelMessages: make(map[string]*storage.ChannelMessage),
    }
}
```

### 11.3 存储工厂

为了简化存储实例的创建，我们提供了一个存储工厂，它可以根据配置创建不同类型的存储实例。

```go
// StorageFactory 创建存储实例
type StorageFactory struct{}

// NewStorage 创建新的存储实例
func (f *StorageFactory) NewStorage(storageType string, config interface{}) (storage.Storage, error) {
    switch storageType {
    case "gorm":
        db, err := gorm.Open(sqlite.Open("nsqite.db"), &gorm.Config{})
        if err != nil {
            return nil, err
        }
        store := gorm.NewGormStorage(db)
        if err := store.Init(config); err != nil {
            return nil, err
        }
        return store, nil
    case "memory":
        store := memory.NewMemoryStorage()
        if err := store.Init(config); err != nil {
            return nil, err
        }
        return store, nil
    default:
        return nil, fmt.Errorf("unsupported storage type: %s", storageType)
    }
}
```

## 12. 总结

基于持久化接口的简单版NSQite保留了NSQ的核心功能和设计理念，同时大大简化了实现。它适合用于学习和理解消息队列的基本原理，也可以用于小规模的生产环境。通过可插拔的持久化接口，系统具有更好的可扩展性和可维护性，可以支持多种数据库后端，同时牺牲了一定的性能。对于大多数中小规模应用，这种权衡是可以接受的。

我们使用 GORM 作为主要的存储实现，它提供了统一的 ORM 接口，支持多种数据库后端。对于 SQLite，我们使用 `github.com/glebarez/sqlite` 作为驱动，这是一个纯 Go 实现的 SQLite 驱动，不需要 CGO 支持，更适合跨平台部署。
