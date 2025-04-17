package nsqite

import "time"

// Option 统一的配置选项
type Option func(*BaseConfig)

// WithQueueSize 设置消息队列大小
// WithQueueSize(0) 表示无缓冲队列
func WithQueueSize(size uint16) Option {
	return func(c *BaseConfig) {
		c.queueSize = size
	}
}

// WithMaxAttempts 设置消息最大重试次数，其次数是最终回调函数执行次数
func WithMaxAttempts(maxAttempts uint32) Option {
	return func(c *BaseConfig) {
		c.maxAttempts = maxAttempts
	}
}

// WithDiscardOnBlocking 设置是否丢弃消息，当队列已满时
func WithDiscardOnBlocking(v bool) Option {
	return func(c *BaseConfig) {
		c.discardOnBlocking = v
	}
}

// WithCheckTimeout 设置消息检查超时时间
func WithCheckTimeout(checkTimeout time.Duration) Option {
	return func(c *BaseConfig) {
		if checkTimeout <= 0 {
			checkTimeout = defaultPubTimeout
		}
		c.checkTimeout = checkTimeout
	}
}
