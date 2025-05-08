package nsqite

import (
	"database/sql"
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
	query := `DELETE FROM nsqite_messages WHERE timestamp < ?`
	thresholdDate := time.Now().AddDate(0, 0, days)
	_, err := d.DB.Exec(query, thresholdDate)
	return err
}

func (d *DB) DeleteCompletedMessagesOlderThan(days int) error {
	query := `DELETE FROM nsqite_messages WHERE responded >= consumers AND timestamp < ?`
	thresholdDate := time.Now().AddDate(0, 0, days)
	_, err := d.DB.Exec(query, thresholdDate)
	return err
}

func (d *DB) FetchPendingMessages(id int, msgs *[]Message) error {
	query := `SELECT id, topic, body, channels, consumers, responded, responded_channels, timestamp, attempts
	          FROM nsqite_messages
	          WHERE id > ? AND responded < consumers
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

func (d *DB) Count() (int, error) {
	query := `SELECT COUNT(id) FROM nsqite_messages`
	var count int
	err := d.DB.QueryRow(query).Scan(&count)
	return count, err
}
