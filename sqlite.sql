-- nsqite.SetDB(db).AutoMigrate()

-- or
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