package db

import (
	"database/sql"
	"log"
	"sync"
)

// KeyID maps a string telemetry/attribute key (e.g. "temperature") to
// the integer key_id used by ts_kv / attribute_kv / ts_kv_latest. The
// dictionary is persistent (postgres `key_dictionary` table) and
// cached in-memory once at boot.
//
// Thread-safe: lookup takes a read lock; insertion takes a write lock
// and double-checks to avoid races on the first reference of a new
// key.

var (
	keyDict   = make(map[string]int)
	keyDictMu sync.RWMutex
)

// LoadKeyDictionary populates the in-memory cache from postgres. Call
// once at boot after Pool is initialized.
func LoadKeyDictionary() {
	if Pool == nil {
		return
	}
	rows, err := Pool.Query("SELECT key, key_id FROM key_dictionary")
	if err != nil {
		log.Printf("ERROR: Failed to load key_dictionary from Postgres: %v", err)
		return
	}
	defer rows.Close()

	keyDictMu.Lock()
	defer keyDictMu.Unlock()

	for rows.Next() {
		var k string
		var id int
		if err := rows.Scan(&k, &id); err == nil {
			keyDict[k] = id
		}
	}
	log.Printf("Loaded %d keys from Postgres key_dictionary.", len(keyDict))
}

// GetOrInsertKeyID returns the key_id for the given string key,
// creating the row in `key_dictionary` if it doesn't exist. Returns
// -1 on failure.
func GetOrInsertKeyID(key string) int {
	if Pool == nil {
		return -1
	}
	keyDictMu.RLock()
	id, ok := keyDict[key]
	keyDictMu.RUnlock()
	if ok {
		return id
	}

	keyDictMu.Lock()
	defer keyDictMu.Unlock()

	if id, ok = keyDict[key]; ok {
		return id
	}

	err := Pool.QueryRow(
		"INSERT INTO key_dictionary (key) VALUES ($1) ON CONFLICT (key) DO NOTHING RETURNING key_id",
		key,
	).Scan(&id)
	if err != nil {
		if err == sql.ErrNoRows {
			err = Pool.QueryRow("SELECT key_id FROM key_dictionary WHERE key = $1", key).Scan(&id)
			if err != nil {
				log.Printf("ERROR: Could not insert or fetch key_dictionary for key %s: %v", key, err)
				return -1
			}
		} else {
			log.Printf("ERROR: Could not insert into key_dictionary for key %s: %v", key, err)
			return -1
		}
	}
	keyDict[key] = id
	return id
}
