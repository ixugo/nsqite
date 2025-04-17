package nsqite

import "time"

const defaultPubTimeout = 3 * time.Second

// BaseConfig 基础配置实现
type BaseConfig struct {
	queueSize    uint16
	maxAttempts  uint32
	checkTimeout time.Duration

	// 仅事件总线有效
	// discardOnBlocking 当消息队列满时是否丢弃消息
	// 如果为 true，当消息队列满时，新消息将被丢弃
	// 如果为 false，当消息队列满时，发布者将阻塞直到队列有空间
	discardOnBlocking bool
}

func newBaseConfig() *BaseConfig {
	return &BaseConfig{
		queueSize:    128,
		maxAttempts:  10,
		checkTimeout: defaultPubTimeout,
	}
}
