package nsqite

import (
	"database/sql"
	"fmt"
	"time"
)

const (
	DriverNameSQLite   = "sqlite"
	DriverNamePostgres = "postgres"
	DriverNameOther    = "other"
)

type DB struct {
	*sql.DB
	driverName string
}

var db *DB

// SetDB init db
// or SetSQLite
// or SetPostgres
func SetDB(driverName string, g *sql.DB) *DB {
	db = &DB{DB: g, driverName: driverName}
	return db
}

// SetSQLite init db
// or SetPostgres
func SetSQLite(g *sql.DB) *DB {
	db = &DB{DB: g, driverName: DriverNameSQLite}
	return db
}

// SetPostgres init db
// or SetSQLite
func SetPostgres(g *sql.DB) *DB {
	db = &DB{DB: g, driverName: DriverNamePostgres}
	return db
}

// AutoMigrate init database table
// 1. create table nsqite_messages if not exists
// 如果你使用 gorm，可使用 gorm.AutoMigrate(new(nsqite.Message)) 初始化
// 如果你使用 goddd，可使用以下方式
//
// if orm.EnabledAutoMigrate {
// if err := uc.DB.AutoMigrate(new(nsqite.Message)); err != nil {
// panic(err)
// }
// }
func (d *DB) AutoMigrate() error {
	var query string
	if d.driverName == DriverNameSQLite {
		query = `
		CREATE TABLE IF NOT EXISTS nsqite_messages (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			topic TEXT NOT NULL DEFAULT '',
			body BLOB NOT NULL,
			consumers INTEGER NOT NULL DEFAULT 0,
			responded INTEGER NOT NULL DEFAULT 0,
			channels TEXT NOT NULL DEFAULT '',
			responded_channels TEXT NOT NULL DEFAULT '',
			attempts INTEGER NOT NULL DEFAULT 0
		);
		CREATE INDEX IF NOT EXISTS idx_messages_consumers_responded ON nsqite_messages (consumers, responded);
		CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON nsqite_messages (timestamp);
		`
	} else {
		query = `
		CREATE TABLE IF NOT EXISTS nsqite_messages (
			id SERIAL PRIMARY KEY,
			timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			topic TEXT NOT NULL DEFAULT '',
			body BYTEA NOT NULL,
			consumers INTEGER NOT NULL DEFAULT 0,
			responded INTEGER NOT NULL DEFAULT 0,
			channels TEXT NOT NULL DEFAULT '',
			responded_channels TEXT NOT NULL DEFAULT '',
			attempts INTEGER NOT NULL DEFAULT 0
		);
		CREATE INDEX IF NOT EXISTS idx_messages_consumers_responded ON nsqite_messages (consumers, responded);
		CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON nsqite_messages (timestamp);
		`
	}
	_, err := d.DB.Exec(query)
	return err
}

func getDB() *DB {
	if db == nil {
		panic("please use nsqite.SetDB() to set db")
	}
	return db
}

func (d *DB) Create(value *Message) error {
	if d.driverName == DriverNamePostgres {
		// PostgreSQL 不支持 LastInsertId，需要使用 RETURNING 子句
		query := `INSERT INTO nsqite_messages (topic, body, channels, consumers, responded, responded_channels, timestamp)
		          VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`
		err := d.DB.QueryRow(query,
			value.Topic,
			value.Body,
			value.Channels,
			value.Consumers,
			value.Responded,
			value.RespondedChannels,
			value.Timestamp,
		).Scan(&value.ID)
		return err
	}

	query := `INSERT INTO nsqite_messages (topic, body, channels, consumers, responded, responded_channels, timestamp)
	          VALUES (?, ?, ?, ?, ?, ?, ?)`
	result, err := d.DB.Exec(query,
		value.Topic,
		value.Body,
		value.Channels,
		value.Consumers,
		value.Responded,
		value.RespondedChannels,
		value.Timestamp,
	)
	if err != nil {
		return err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	value.ID = int(id)
	return nil
}

func (d *DB) DeleteOldMessages(days int) error {
	query := `DELETE FROM nsqite_messages WHERE timestamp < ` + d.placeholder(1)
	thresholdDate := time.Now().AddDate(0, 0, days)
	_, err := d.DB.Exec(query, thresholdDate)
	return err
}

func (d *DB) DeleteCompletedMessagesOlderThan(days int) error {
	query := `DELETE FROM nsqite_messages WHERE responded >= consumers AND timestamp < ` + d.placeholder(1)
	thresholdDate := time.Now().AddDate(0, 0, days)
	_, err := d.DB.Exec(query, thresholdDate)
	return err
}

func (d *DB) FetchPendingMessages(id int, msgs *[]Message) error {
	query := `SELECT id, topic, body, channels, consumers, responded, responded_channels, timestamp, attempts
	          FROM nsqite_messages
	          WHERE id > ` + d.placeholder(1) + ` AND responded < consumers
	          ORDER BY id ASC
	          LIMIT 100`
	rows, err := d.DB.Query(query, id)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var msg Message
		if err := rows.Scan(&msg.ID, &msg.Topic, &msg.Body, &msg.Channels, &msg.Consumers, &msg.Responded, &msg.RespondedChannels, &msg.Timestamp, &msg.Attempts); err != nil {
			return err
		}
		*msgs = append(*msgs, msg)
	}
	return rows.Err()
}

func (d *DB) DriverName() string {
	return d.driverName
}

// placeholder 根据数据库驱动类型返回对应的占位符
// SQLite 使用 ? 占位符，PostgreSQL 使用 $1, $2, $3... 位置参数
func (d *DB) placeholder(index int) string {
	if d.driverName == DriverNamePostgres {
		return fmt.Sprintf("$%d", index)
	}
	return "?"
}

// placeholders 生成多个占位符，用逗号分隔
// 用于 INSERT 语句的 VALUES 部分
func (d *DB) placeholders(count int) string {
	if count <= 0 {
		return ""
	}
	if d.driverName == DriverNamePostgres {
		result := ""
		for i := 1; i <= count; i++ {
			if i > 1 {
				result += ", "
			}
			result += fmt.Sprintf("$%d", i)
		}
		return result
	}
	result := "?"
	for i := 1; i < count; i++ {
		result += ", ?"
	}
	return result
}

func (d *DB) Count() (int, error) {
	query := `SELECT COUNT(id) FROM nsqite_messages`
	var count int
	err := d.DB.QueryRow(query).Scan(&count)
	return count, err
}
