// Package dbutil holds the small SQL marshalling helpers that every CRUD
// handler reaches for: empty-string-to-NULL coercion, the TB nil-UUID
// sentinel, and the JSON-marshal-or-NULL pattern used for jsonb columns.
//
// These were package-main globals before the handler extraction. Keeping
// them out of httputil because they're tied to the database driver, not
// the HTTP layer.
package dbutil

import (
	"database/sql"
	"encoding/json"
)

// nilUUID is what TB classic stores in customer_id / external_id columns
// when the relation is intentionally absent. Treating it as NULL on the
// way in keeps row counts honest.
const nilUUID = "13814000-1dd2-11b2-8080-808080808080"

// NullStr returns sql.NullString{Valid: false} for empty input, otherwise
// a Valid string. Use for any varchar column where "" should land as NULL.
func NullStr(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

// NullUUID is NullStr but also treats TB's nil-UUID sentinel as NULL.
func NullUUID(s string) sql.NullString {
	if s == "" || s == nilUUID {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

// JSONOrNil marshals v as JSON, returning the string for jsonb columns.
// nil input or marshal failure returns nil so the driver writes NULL.
func JSONOrNil(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return string(b)
}

// ParsePgTextArray decodes Postgres' wire format for TEXT[] — `{a,b,"c d"}`
// — into a Go slice. Strips the surrounding braces, splits on the commas
// outside double quotes, and unwraps any quoted element. Returns an empty
// slice on malformed input rather than erroring; the array columns we
// query (widget tags, propagate relation types) are tolerable as empty.
func ParsePgTextArray(raw []byte) []string {
	s := string(raw)
	if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {
		return []string{}
	}
	inner := s[1 : len(s)-1]
	if inner == "" {
		return []string{}
	}
	out := []string{}
	cur := []byte{}
	inQuotes := false
	flush := func() {
		v := string(cur)
		if len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
			v = v[1 : len(v)-1]
		}
		out = append(out, v)
		cur = cur[:0]
	}
	for i := 0; i < len(inner); i++ {
		c := inner[i]
		if c == '"' {
			inQuotes = !inQuotes
			cur = append(cur, c)
			continue
		}
		if c == ',' && !inQuotes {
			flush()
			continue
		}
		cur = append(cur, c)
	}
	if len(cur) > 0 {
		flush()
	}
	return out
}
