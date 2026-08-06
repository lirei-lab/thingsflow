package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/migrations"
	"flow-core/internal/twinstore"
)

// InitPostgres opens the Postgres pool via internal/db, then runs
// migrations and pre-loads the key_dictionary.
func InitPostgres() {
	pool, err := dbpkg.Init()
	if err != nil {
		log.Printf("ERROR: postgres init: %v", err)
		return
	}

	log.Println("PostgreSQL connection established for operational storage.")
	if err := runMigrationsWithStartupTimeout(pool); err != nil {
		if strings.Contains(err.Error(), "startup migration check timed out") {
			log.Printf("WARN: %v", err)
		} else {
			log.Printf("ERROR: schema migrations failed: %v", err)
		}
	}
	dbpkg.LoadKeyDictionary()
}

func runMigrationsWithStartupTimeout(pool *sql.DB) error {
	timeout := migrationStartupTimeout()
	done := make(chan error, 1)
	go func() {
		done <- migrations.Run(pool)
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("startup migration check timed out after %s; continuing with existing schema", timeout)
	}
}

func migrationStartupTimeout() time.Duration {
	seconds, err := strconv.Atoi(os.Getenv("FLOW_MIGRATION_STARTUP_TIMEOUT_SECONDS"))
	if err != nil || seconds <= 0 {
		seconds = 10
	}
	return time.Duration(seconds) * time.Second
}

func toUUID(msb, lsb int64) string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uint64(msb)>>32,
		(uint64(msb)>>16)&0xFFFF,
		uint64(msb)&0xFFFF,
		uint64(lsb)>>48,
		uint64(lsb)&0xFFFFFFFFFFFF)
}

func SaveAttributes(tenantId, deviceId string, data map[string]interface{}) {
	if store := twinstore.Global(); store != nil {
		if err := store.MergeAttributes(context.Background(), tenantId, "DEVICE", deviceId, "CLIENT_SCOPE", time.Now().UnixMilli(), data); err != nil {
			log.Printf("WARN: Failed to save attributes to twin state for device %s: %v", deviceId, err)
		}
	}
	if dbpkg.Pool == nil {
		return
	}

	ts := time.Now().UnixMilli()
	log.Printf("DEBUG: SaveAttributes called for device %s with %d keys", deviceId, len(data))

	for key, value := range data {
		keyId := dbpkg.GetOrInsertKeyID(key)
		if keyId == -1 {
			continue
		}

		var boolV *bool
		var strV *string
		var longV *int64
		var dblV *float64
		var jsonV *string

		switch v := value.(type) {
		case bool:
			boolV = &v
		case int:
			val := int64(v)
			longV = &val
		case int64:
			longV = &v
		case float64:
			dblV = &v
		case string:
			strV = &v
		default:
			s := fmt.Sprintf("%v", v)
			strV = &s
		}

		// attribute_type = 0 is CLIENT_SCOPE
		query := `INSERT INTO attribute_kv (entity_id, attribute_type, attribute_key, bool_v, str_v, long_v, dbl_v, json_v, last_update_ts)
				  VALUES ($1, 0, $2, $3, $4, $5, $6, $7, $8)
				  ON CONFLICT (entity_id, attribute_type, attribute_key) DO UPDATE SET
				  bool_v = EXCLUDED.bool_v, str_v = EXCLUDED.str_v, long_v = EXCLUDED.long_v,
				  dbl_v = EXCLUDED.dbl_v, json_v = EXCLUDED.json_v, last_update_ts = EXCLUDED.last_update_ts`

		_, err := dbpkg.Pool.Exec(query, deviceId, keyId, boolV, strV, longV, dblV, jsonV, ts)
		if err != nil {
			log.Printf("WARN: Failed to save client attribute to PostgreSQL for device %s key %s: %v", deviceId, key, err)
		}
	}

	// No direct WS broadcast: the MergeAttributes at the top of this
	// function makes the twin KV watch (twin_state.go) observe the write —
	// the watch is the single publisher of attribute pushes (milestone 3
	// phase 1; a direct call here would double every push).
}

func fetchAttributes(deviceId string, attrType int, keys []string, dest map[string]interface{}) {
	if len(keys) == 0 {
		return
	}

	placeholders := make([]string, len(keys))
	args := make([]interface{}, len(keys)+2)
	args[0] = deviceId
	args[1] = attrType

	for i, key := range keys {
		placeholders[i] = fmt.Sprintf("$%d", i+3)
		args[i+2] = key
	}

	query := fmt.Sprintf(`SELECT k.key, a.bool_v, a.str_v, a.long_v, a.dbl_v, a.json_v
						  FROM attribute_kv a
						  JOIN key_dictionary k ON a.attribute_key = k.key_id
						  WHERE a.entity_id = $1 AND a.attribute_type = $2 AND k.key IN (%s)`, strings.Join(placeholders, ","))

	rows, err := dbpkg.Pool.Query(query, args...)
	if err != nil {
		log.Printf("Error querying attributes for device %s: %v", deviceId, err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var key string
		var boolV *bool
		var strV *string
		var longV *int64
		var dblV *float64
		var jsonV *string

		if err := rows.Scan(&key, &boolV, &strV, &longV, &dblV, &jsonV); err != nil {
			continue
		}

		if boolV != nil {
			dest[key] = *boolV
		} else if strV != nil {
			dest[key] = *strV
		} else if longV != nil {
			dest[key] = *longV
		} else if dblV != nil {
			dest[key] = *dblV
		} else if jsonV != nil {
			var parsed interface{}
			json.Unmarshal([]byte(*jsonV), &parsed)
			dest[key] = parsed
		}
	}
}

// lookupDeviceForRPC resolves a device id to the MQTT identity its commands must
// be addressed to, plus the owning tenant so the caller can enforce isolation.
//
// The identity is derived the same way devicejwt does when it mints the token's
// `clientid` claim — strip the separators from the uuid. Deriving it here rather
// than storing it keeps a single source of truth: if the two ever disagreed, the
// broker ACL would silently reject every command as a foreign topic.
func lookupDeviceForRPC(deviceID string) (string, string, error) {
	var tenantID string
	if err := dbpkg.Pool.QueryRow(
		"SELECT tenant_id::text FROM device WHERE id = $1", deviceID).Scan(&tenantID); err != nil {
		return "", "", err
	}
	replacer := strings.NewReplacer("-", "", "_", "", ":", "", "/", "")
	return replacer.Replace(strings.TrimSpace(deviceID)), tenantID, nil
}
