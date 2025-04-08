package nsqite

import "errors"

// 定义错误
var (
	// ErrNoHandler 表示没有设置消息处理函数
	ErrNoHandler = errors.New("没有设置消息处理函数")
)
