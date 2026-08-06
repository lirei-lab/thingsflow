package main

import (
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

// SaveAttributes (the CLIENT_SCOPE device write helper) was removed here:
// zero callers remained after the transport write path moved to the data
// plane (Envoy/Bento — device POSTs are rejected by flow-core), so keeping
// it invited an unaudited attribute write path back in.

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
