package nsqite

import "gorm.io/gorm"

// Transaction messages are persisted using gorm
// If you don't want to use orm, please raise an issue and we can discuss implementation alternatives
// 事务消息基于 gorm 做的持久化
// 如果不想使用 orm，可以提出 issus，咱们讨论如何实现

var db *gorm.DB

func SetGorm(g *gorm.DB) {
	db = g
}

func gormDB() *gorm.DB {
	if db == nil {
		panic("please use nsqite.SetGorm() to set db")
	}
	return db
}
