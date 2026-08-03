package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/lib/pq"

	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/deviceactivity"
	"flow-core/internal/telemetry"
	"flow-core/internal/topology"
	"flow-core/internal/twinstore"
)

// maxWalkNodes bounds a single relationsQuery BFS. The walk issues 1-3 SQL
// queries per visited node, so maxLevel alone does not bound the work a single
// WS command can trigger on a densely connected graph. A var (not a const) so
// tests can shrink it instead of seeding 5000 rows.
var maxWalkNodes = 5000

// Phase 5c resource bounds for the WS plane.
//
// maxWSMessageBytes caps a single inbound frame. gorilla's default read limit
// is UNLIMITED and the http.Server body cap does not survive the hijack, so
// without this one authenticated client can stream a multi-GB frame that
// ReadMessage buffers whole → OOM on a 1Gi pod. 1 MiB is ~20× the largest
// legitimate client message: the biggest thing the TB UI sends is an
// ENTITY_DATA subscription batch (a dashboard page with ~50 widgets over ~200
// entities serialises to well under 50 KB), so the cap only stops abuse.
//
// wsPongWait is the read deadline. It is refreshed on EVERY successful read
// AND on every pong, so a long-lived idle dashboard survives indefinitely as
// long as the browser answers pings (every WS client answers pings at the
// protocol level). A half-open connection — client gone without a FIN, e.g.
// laptop lid closed on wifi — is reaped after at most wsPongWait instead of
// leaking a goroutine plus its session/subscription maps forever.
//
// wsPingInterval must stay comfortably below wsPongWait so a couple of dropped
// pings do not kill a healthy session: 30s ping vs 90s deadline tolerates two
// consecutive losses.
const (
	maxWSMessageBytes = 1 << 20 // 1 MiB
	wsPongWait        = 90 * time.Second
	wsPingInterval    = 30 * time.Second
	wsWriteWait       = 10 * time.Second
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     checkOrigin,
}

// checkOrigin gates the WS handshake on the Origin header instead of the old
// unconditional `return true`. Policy mirrors setCORSHeaders in package main:
//
//   - ALLOWED_ORIGIN unset or "*" → permissive (dev default). Production MUST
//     set ALLOWED_ORIGIN to the UI's exact origin to close cross-site WS.
//   - a request with no Origin header is a non-browser client (device/SDK/CLI)
//     and is not subject to the browser same-origin/CSWSH threat → allowed.
//   - otherwise the Origin must equal ALLOWED_ORIGIN exactly.
func checkOrigin(r *http.Request) bool {
	allowed := strings.TrimSpace(os.Getenv("ALLOWED_ORIGIN"))
	if allowed == "" || allowed == "*" {
		return true
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	return origin == allowed
}

// WsRequest is the parsed JSON envelope from the ThingsBoard UI client.
// TB v3.7+ uses the unified "cmds" array; older versions use tsSubCmds, latestSubCmds, etc.
type WsRequest struct {
	// Legacy format (TB < 3.7)
	TsSubCmds     []WsCmd `json:"tsSubCmds"`
	LatestSubCmds []WsCmd `json:"latestSubCmds"`
	AlarmSubCmds  []WsCmd `json:"alarmSubCmds"`
	AttrSubCmds   []WsCmd `json:"attrSubCmds"`

	// New format (TB 3.7+)
	Cmds    []WsCmd                 `json:"cmds"`
	AuthCmd *map[string]interface{} `json:"authCmd,omitempty"`
}

// WsCmd is a single subscription command from the ThingsBoard UI.
// Supports both legacy and v3.7+ fields.
type WsCmd struct {
	EntityId    string                 `json:"entityId"`
	EntityType  string                 `json:"entityType"`
	Keys        string                 `json:"keys"`
	CmdId       int                    `json:"cmdId"`
	Scope       string                 `json:"scope"`
	Type        string                 `json:"type"`
	StartTs     int64                  `json:"startTs"`
	TimeWindow  int64                  `json:"timeWindow"`
	Limit       int                    `json:"limit"`
	Agg         string                 `json:"agg"`
	Interval    int64                  `json:"interval"`
	Query       map[string]interface{} `json:"query"`
	Unsubscribe bool                   `json:"unsubscribe"`
}

// Subscription tracks a single subscription's cmdId and key filter.
type Subscription struct {
	CmdId int
	Keys  []string
}

// EntityRef captures the id+type that a multi-step ENTITY_DATA flow refers to,
// so the second message (historyCmd/tsCmd without a full query) can recover
// both fields when building its update payload.
type EntityRef struct {
	ID   string
	Type string
	// Keys captures the data-key set the ENTITY_DATA cmd subscribed
	// to (from tsCmd.keys / latestCmd.keys / query.latestValues). The
	// broadcaster MUST filter pushes to this set — TB UI v4 builds an
	// internal dataKeys lookup keyed by `${name}_${type}` and crashes
	// with "a is not iterable" if we push a key the widget didn't ask
	// for. Empty Keys means "no filter known yet" — push everything.
	Keys []string
}

// Session holds a WebSocket connection and its active subscriptions.
type Session struct {
	Conn          *websocket.Conn
	Subs          map[string][]Subscription // entityId -> telemetry subscriptions
	AlarmSubs     map[string][]Subscription // entityId -> alarm subscriptions
	AttrSubs      map[string][]Subscription // entityId -> attribute subscriptions
	EntityCmdMap  map[int]EntityRef         // cmdId -> entity ref for ENTITY_DATA two-step flow
	TenantID      string                    // authenticated tenant ID (from the verified JWT claim)
	SysAdmin      bool                      // true when the verified JWT carries the SYS_ADMIN scope
	Authenticated bool                      // true once a valid authCmd JWT has been seen
	mu            sync.Mutex
}

// tenantScope is the authorisation context every WS read must be evaluated
// against. It is derived ONLY from the session's verified JWT claims (see the
// authCmd branch in HandleWebSocket) — never from a command payload, which is
// attacker-controlled.
//
// Rules:
//   - TenantID == "" → the session may read NOTHING (fail closed). This is the
//     deny-by-default half of the WS IDOR fix: an unauthenticated/tenant-less
//     session must never fall through to a query with no tenant predicate.
//   - SysAdmin → may cross tenants, mirroring the HTTP telemetry readers.
type tenantScope struct {
	TenantID string
	SysAdmin bool
}

// sessionScope snapshots the session's verified identity under its mutex.
func sessionScope(s *Session) tenantScope {
	s.mu.Lock()
	defer s.mu.Unlock()
	return tenantScope{TenantID: s.TenantID, SysAdmin: s.SysAdmin}
}

// tenantOwnedTable maps a TB entity type to the operational table that carries
// its owning tenant_id. Unknown types return ok=false so callers fail closed —
// an entity type we cannot prove ownership for is never readable.
//
// The returned name is a compile-time constant from this switch, so callers may
// interpolate it into SQL; the ids/tenants themselves stay bind parameters.
func tenantOwnedTable(entityType string) (string, bool) {
	switch strings.ToUpper(strings.TrimSpace(entityType)) {
	case "DEVICE":
		return "device", true
	case "ASSET":
		return "asset", true
	case "ENTITY_VIEW":
		return "entity_view", true
	case "CUSTOMER":
		return "customer", true
	case "DASHBOARD":
		return "dashboard", true
	case "USER":
		return "tb_user", true
	case "EDGE":
		return "edge", true
	case "ALARM":
		return "alarm", true
	case "RULE_CHAIN":
		return "rule_chain", true
	case "API_USAGE_STATE":
		// The API_USAGE_STATE entity id is the api_usage_state row's own PK;
		// the row carries the owning tenant_id directly.
		return "api_usage_state", true
	default:
		return "", false
	}
}

// entityBelongsToTenant verifies that entityId is owned by tenantID.
//
// The WS hot tables (ts_kv, ts_kv_latest, attribute_kv) are keyed by entity_id
// with no tenant column of their own, so the only tenant boundary available is
// the entity's own owning tenant, resolved here from the entity's table. Same
// idea as internal/telemetry.entityBelongsToTenant; kept local (that one is
// unexported) so this fix touches no Phase 1-5a package.
//
// Unknown entity types, missing rows and query errors all return false
// (fail-closed).
func entityBelongsToTenant(entityType, entityId, tenantID string) bool {
	if dbpkg.Pool == nil || entityId == "" || tenantID == "" {
		return false
	}
	// A TENANT entity owns itself.
	if strings.EqualFold(strings.TrimSpace(entityType), "TENANT") {
		return entityId == tenantID
	}
	table, ok := tenantOwnedTable(entityType)
	if !ok {
		return false
	}
	var owner string
	if err := dbpkg.Pool.QueryRow(
		"SELECT COALESCE(tenant_id::text, '') FROM "+table+" WHERE id = $1", entityId,
	).Scan(&owner); err != nil {
		return false
	}
	return owner == tenantID
}

// allows reports whether this scope may read the given entity. Deny-by-default:
// no tenant → nothing; SYS_ADMIN → anything; otherwise the entity must be owned
// by the session's tenant.
func (ts tenantScope) allows(entityType, entityId string) bool {
	if ts.TenantID == "" || entityId == "" {
		return false
	}
	if ts.SysAdmin {
		return true
	}
	return entityBelongsToTenant(entityType, entityId, ts.TenantID)
}

var sessionManager = struct {
	sync.RWMutex
	sessions map[*Session]bool
}{sessions: make(map[*Session]bool)}

// BroadcastTelemetry sends a telemetry update to every WS subscriber
// of the given entity. Two subscription channels are honoured:
//
//   - Legacy session.Subs (pre-v3.7 tsSubCmds / latestSubCmds): values
//     come back as stringified pairs `[ts, "42.5"]` per the original
//     TB protocol.
//   - session.EntityCmdMap (TB v3.7+/v4 ENTITY_DATA flow): values keep
//     their native JSON type and are wrapped in the ENTITY_DATA
//     update envelope `{cmdUpdateType: "ENTITY_DATA", update: [...]}`.
//
// Without the second branch, the chart widgets in TB UI v4.x only
// rendered the data fetched at subscription time — refreshing the
// browser worked because that re-runs the initial query, but live
// pushes never arrived since EntityCmdMap subscriptions weren't
// included in the broadcast loop.
func BroadcastTelemetry(entityId string, data map[string]interface{}, ts int64) {
	sessionManager.RLock()
	defer sessionManager.RUnlock()

	for session := range sessionManager.sessions {
		session.mu.Lock()

		// Legacy v3.x subscriptions.
		if subs, ok := session.Subs[entityId]; ok {
			for _, sub := range subs {
				dataMap := map[string]interface{}{}
				for k, v := range data {
					if len(sub.Keys) == 0 || contains(sub.Keys, k) {
						strVal := ""
						switch t := v.(type) {
						case string:
							strVal = t
						default:
							b, _ := json.Marshal(v)
							strVal = string(b)
						}
						dataMap[k] = [][]interface{}{{ts, strVal}}
					}
				}
				if len(dataMap) > 0 {
					_ = session.Conn.WriteJSON(map[string]interface{}{
						"subscriptionId": sub.CmdId,
						"data":           dataMap,
					})
				}
			}
		}

		// v3.7+/v4 ENTITY_DATA subscriptions. We populate BOTH
		// `latest.TIME_SERIES` (gauge / latest-value widgets) and
		// `timeseries` (chart widgets that draw a series). The TB UI
		// reads whichever channel its widget type expects and ignores
		// the other, so emitting both makes the broadcast widget-
		// agnostic.
		//
		// Filter keys to ref.Keys: TB UI v4 maintains an internal
		// dataKeys lookup populated at subscription time. Pushing a
		// key it never registered makes timeseriesDataKeysByKeyNames
		// crash with "a is not iterable". When ref.Keys is empty
		// (subscription captured no key list — early-format clients)
		// we push everything for back-compat.
		for cmdId, ref := range session.EntityCmdMap {
			if ref.ID != entityId {
				continue
			}
			keyFilter := map[string]struct{}{}
			if len(ref.Keys) > 0 {
				for _, k := range ref.Keys {
					keyFilter[k] = struct{}{}
				}
			}
			tsLatest := map[string]interface{}{}
			tsSeries := map[string]interface{}{}
			for k, v := range data {
				if len(keyFilter) > 0 {
					if _, ok := keyFilter[k]; !ok {
						continue
					}
				}
				tsLatest[k] = map[string]interface{}{
					"ts":    ts,
					"value": v,
				}
				tsSeries[k] = []map[string]interface{}{
					{"ts": ts, "value": v},
				}
			}
			if len(tsLatest) == 0 {
				continue
			}
			_ = session.Conn.WriteJSON(map[string]interface{}{
				"cmdId":         cmdId,
				"errorCode":     0,
				"errorMsg":      nil,
				"cmdUpdateType": "ENTITY_DATA",
				"data":          nil,
				"update": []map[string]interface{}{
					{
						"entityId": map[string]interface{}{
							"entityType": ref.Type,
							"id":         entityId,
						},
						"latest": map[string]interface{}{
							"TIME_SERIES": tsLatest,
						},
						"timeseries": tsSeries,
						"aggLatest":  map[string]interface{}{},
					},
				},
			})
		}

		session.mu.Unlock()
	}
}

// BroadcastAlarmEvent sends an alarm state change event to all sessions
// subscribed to alarms for the given entity.
func BroadcastAlarmEvent(entityId string, event map[string]interface{}) {
	sessionManager.RLock()
	defer sessionManager.RUnlock()

	for session := range sessionManager.sessions {
		session.mu.Lock()
		subs, ok := session.AlarmSubs[entityId]
		if ok {
			for _, sub := range subs {
				payload := map[string]interface{}{
					"subscriptionId": sub.CmdId,
					"alarm":          event,
				}
				if err := session.Conn.WriteJSON(payload); err != nil {
					log.Printf("Error sending WS alarm payload: %v", err)
				}
			}
		}
		// Also broadcast to telemetry sessions as a notification — many TB widgets
		// listen for alarms on the telemetry channel when no explicit alarmSubCmds is used.
		if _, hasTsSubs := session.Subs[entityId]; hasTsSubs {
			payload := map[string]interface{}{
				"alarmNotification": event,
			}
			if err := session.Conn.WriteJSON(payload); err != nil {
				log.Printf("Error sending WS alarm notification: %v", err)
			}
		}
		session.mu.Unlock()
	}

	log.Printf("DEBUG: BroadcastAlarmEvent entity=%s event=%v", entityId, event)
}

func contains(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

// filterOwnedSubCmds drops every subscribe command whose target entity is not
// owned by the session's tenant. Unsubscribe commands pass through untouched
// (removing a subscription can never leak) and so do empty entity ids, which
// the caller already skips.
//
// cmd.EntityType is the client's claim about the entity; it only ever narrows
// which table we prove ownership against — the tenant itself always comes from
// the verified JWT via scope. An absent entityType means DEVICE, matching the
// legacy TB clients that omit it.
func filterOwnedSubCmds(scope tenantScope, cmds []WsCmd, channel string) []WsCmd {
	if len(cmds) == 0 {
		return cmds
	}
	out := cmds[:0]
	for _, cmd := range cmds {
		if cmd.EntityId == "" || cmd.Unsubscribe {
			out = append(out, cmd)
			continue
		}
		entityType := cmd.EntityType
		if entityType == "" {
			entityType = "DEVICE"
		}
		if !scope.allows(entityType, cmd.EntityId) {
			log.Printf("WS subscribe denied (%s): entity=%s type=%s not owned by tenant=%q cmdId=%d",
				channel, cmd.EntityId, entityType, scope.TenantID, cmd.CmdId)
			continue
		}
		out = append(out, cmd)
	}
	return out
}

// shortID truncates an entity id for logs without assuming a UUID length.
func shortID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

// removeSubByCmdId returns subs with the entry matching cmdId removed.
func removeSubByCmdId(subs []Subscription, cmdId int) []Subscription {
	out := subs[:0]
	for _, s := range subs {
		if s.CmdId != cmdId {
			out = append(out, s)
		}
	}
	return out
}

// listEntitiesForType pulls a page of rows from the entity table matching
// the given TB entity type. Used by WS ENTITY_DATA list-mode filters.
//
// typeFilter restricts asset.type / device.type to a fixed set (TB v4
// dashboards send `assetTypes: ["building"]` etc.). Empty slice = no
// type filter. nameFilter is a case-insensitive substring on
// asset/device.name (TB UI search box). Without these the dashboard
// alias for "Buildings" returned the entire fleet (district + floors
// + zones) and widget filters could not narrow it down.
func listEntitiesForType(tenantId, entityType string, typeFilter []string, nameFilter string, limit, offset int) []map[string]interface{} {
	if dbpkg.Pool == nil || tenantId == "" {
		return nil
	}
	var (
		base    string
		hasType bool
	)
	switch entityType {
	case "DEVICE":
		base = `SELECT id, created_time, name, COALESCE(label,'') AS label, type FROM device WHERE tenant_id = $1`
		hasType = true
	case "ASSET":
		base = `SELECT id, created_time, name, COALESCE(label,'') AS label, type FROM asset WHERE tenant_id = $1`
		hasType = true
	case "CUSTOMER":
		base = `SELECT id, created_time, title AS name, '' AS label, '' AS type FROM customer WHERE tenant_id = $1 AND title != 'Public'`
	case "DASHBOARD":
		base = `SELECT id, created_time, title AS name, '' AS label, '' AS type FROM dashboard WHERE tenant_id = $1`
	case "USER":
		base = `SELECT id, created_time, email AS name, '' AS label, '' AS type FROM tb_user WHERE tenant_id = $1`
	case "ENTITY_VIEW":
		base = `SELECT id, created_time, name, '' AS label, type FROM entity_view WHERE tenant_id = $1`
	default:
		return nil
	}
	args := []interface{}{tenantId}
	if hasType && len(typeFilter) > 0 {
		args = append(args, pq.Array(typeFilter))
		base += fmt.Sprintf(" AND type = ANY($%d)", len(args))
	}
	if nameFilter != "" {
		args = append(args, "%"+strings.ToLower(nameFilter)+"%")
		// Customer/Dashboard use title/email — already aliased to name.
		base += fmt.Sprintf(" AND LOWER(name) LIKE $%d", len(args))
	}
	args = append(args, limit, offset)
	query := base + fmt.Sprintf(" ORDER BY created_time DESC LIMIT $%d OFFSET $%d", len(args)-1, len(args))
	rows, err := dbpkg.Pool.Query(query, args...)
	if err != nil {
		log.Printf("WARN listEntitiesForType type=%s err=%v", entityType, err)
		return nil
	}
	defer rows.Close()

	var out []map[string]interface{}
	for rows.Next() {
		var id, name, label, etype string
		var createdTime int64
		if err := rows.Scan(&id, &createdTime, &name, &label, &etype); err != nil {
			continue
		}
		out = append(out, map[string]interface{}{
			"id":          id,
			"createdTime": createdTime,
			"name":        name,
			"label":       label,
			"type":        etype,
		})
	}
	return out
}

// walkRelationsQuery does a BFS over the `relation` table starting at
// rootID/rootType, following relations of the requested types in the
// given direction (FROM = outgoing, TO = incoming) up to maxLevel hops.
// Yields entities matching allowedTypes (e.g. ASSET) with their
// ENTITY_FIELD + latest ATTRIBUTE values, ready to drop straight into
// an ENTITY_DATA response. Filtering happens at SQL+walker level so a
// drill-down from a Pavillon doesn't accidentally collect entities
// from a sibling Pavillon (they share no relation path).
//
// SECURITY: rootID/rootType come straight off the WS command payload, i.e.
// they are attacker-controlled. The walk is therefore gated twice:
//
//  1. the root must belong to the session's tenant (SYS_ADMIN may cross),
//     otherwise a session could enumerate another tenant's relation graph —
//     names, labels, types and SERVER_SCOPE attributes;
//  2. hydration (loadEntityRow / fetchEntityAttributes) is itself tenant
//     scoped, so even a cross-tenant edge in the graph yields no row.
//
// The walk is also bounded by a total visited-node budget on top of the
// caller's maxLevel clamp: it issues 1-3 queries per node, so an unbounded
// graph is a DoS lever.
func walkRelationsQuery(scope tenantScope, rootID, rootType, direction string, maxLevel int,
	allowedRels map[string]bool, allowedTypes map[string]bool,
	entityFields []struct {
		Type string `json:"type"`
		Key  string `json:"key"`
	},
	attrKeys []string) []map[string]interface{} {
	if dbpkg.Pool == nil || rootID == "" {
		return nil
	}
	if !scope.allows(rootType, rootID) {
		log.Printf("WARN walkRelationsQuery denied: root=%s type=%s not owned by tenant=%q",
			rootID, rootType, scope.TenantID)
		return nil
	}
	relCols := []string{}
	for r := range allowedRels {
		relCols = append(relCols, r)
	}
	type node struct {
		ID    string
		Type  string
		Level int
	}
	visited := map[string]bool{rootID: true}
	queue := []node{{ID: rootID, Type: rootType, Level: 0}}
	hits := []node{}
	budgetHit := false
	for len(queue) > 0 && !budgetHit {
		cur := queue[0]
		queue = queue[1:]
		if cur.Level >= maxLevel {
			continue
		}
		neighbors, err := topology.Neighbors(dbpkg.Pool, topology.EntityRef{ID: cur.ID, Type: cur.Type}, direction, relCols)
		if err != nil {
			log.Printf("WARN walkRelationsQuery query: %v", err)
			continue
		}
		for _, neighbor := range neighbors {
			// Total visited-node budget, checked before each admission so the walk
			// can never exceed it. maxLevel alone does not bound the work: a
			// wide graph fans out arbitrarily at each level.
			if len(visited) >= maxWalkNodes {
				log.Printf("WARN walkRelationsQuery budget exhausted: root=%s visited=%d (cap %d)",
					shortID(rootID), len(visited), maxWalkNodes)
				budgetHit = true
				break
			}
			nid := neighbor.ID
			ntype := neighbor.Type
			if visited[nid] {
				continue
			}
			visited[nid] = true
			// topology.Neighbors reads the edge tables without a tenant
			// predicate, so confine the walk here: a node the session does not
			// own is neither reported nor traversed through. Without this a
			// single cross-tenant edge would open the whole graph behind it.
			if !scope.allows(ntype, nid) {
				continue
			}
			child := node{ID: nid, Type: ntype, Level: cur.Level + 1}
			queue = append(queue, child)
			if len(allowedTypes) == 0 || allowedTypes[ntype] {
				hits = append(hits, child)
			}
		}
	}

	// Hydrate each hit with name/label/type + requested attributes. Both
	// hydration reads are tenant scoped, so a node reached through a
	// cross-tenant edge is silently dropped (row == nil) instead of leaking.
	out := make([]map[string]interface{}, 0, len(hits))
	for _, h := range hits {
		row := loadEntityRow(scope, h.Type, h.ID)
		if row == nil {
			continue
		}
		latest := map[string]interface{}{
			"ENTITY_FIELD": buildEntityFieldLatest(row, entityFields),
		}
		if len(attrKeys) > 0 {
			if attrs := fetchEntityAttributes(scope, h.Type, h.ID, attrKeys); len(attrs) > 0 {
				latest["ATTRIBUTE"] = attrs
			}
		}
		out = append(out, map[string]interface{}{
			"entityId":   map[string]interface{}{"entityType": h.Type, "id": h.ID},
			"latest":     latest,
			"timeseries": map[string]interface{}{},
			"aggLatest":  map[string]interface{}{},
		})
	}
	return out
}

// loadEntityRow fetches one row's name/label/type fields for hydrating
// relations-walk results. Mirrors the SELECT shape that
// listEntitiesForType returns so buildEntityFieldLatest works on it.
//
// The read is tenant scoped: every table here carries tenant_id, so the
// predicate goes into the SQL itself rather than a second ownership query.
// A SYS_ADMIN scope drops the predicate; an empty tenant reads nothing.
func loadEntityRow(scope tenantScope, entityType, id string) map[string]interface{} {
	if dbpkg.Pool == nil || id == "" || scope.TenantID == "" {
		return nil
	}
	var (
		createdTime        int64
		name, label, etype string
	)
	// tenantPred is appended to each SELECT below; args carries the matching
	// bind parameters. SYS_ADMIN reads unscoped, everyone else is confined.
	tenantPred := " AND tenant_id = $2"
	args := []interface{}{id, scope.TenantID}
	if scope.SysAdmin {
		tenantPred = ""
		args = []interface{}{id}
	}
	var err error
	switch entityType {
	case "ASSET":
		err = dbpkg.Pool.QueryRow(
			`SELECT created_time, name, COALESCE(label,''), COALESCE(type,'') FROM asset WHERE id = $1`+tenantPred,
			args...,
		).Scan(&createdTime, &name, &label, &etype)
	case "DEVICE":
		err = dbpkg.Pool.QueryRow(
			`SELECT created_time, name, COALESCE(label,''), COALESCE(type,'') FROM device WHERE id = $1`+tenantPred,
			args...,
		).Scan(&createdTime, &name, &label, &etype)
	case "ENTITY_VIEW":
		err = dbpkg.Pool.QueryRow(
			`SELECT created_time, name, '', COALESCE(type,'') FROM entity_view WHERE id = $1`+tenantPred,
			args...,
		).Scan(&createdTime, &name, &label, &etype)
	default:
		return nil
	}
	if err != nil {
		return nil
	}
	return map[string]interface{}{
		"id":           id,
		"created_time": createdTime,
		"name":         name,
		"label":        label,
		"type":         etype,
	}
}

// fetchEntityAttributes loads the latest SERVER_SCOPE attribute values
// for the requested keys on a single entity. Returns the map shaped
// for TB's `latest.ATTRIBUTE` channel: {key: {ts, value}}. Values keep
// their native JSON type so the strict-types parser in TB UI v4
// renders them — a string-stringified payload is silently dropped.
//
// attribute_kv is keyed by entity_id and has no tenant column, so tenant
// isolation is expressed as an EXISTS ownership predicate against the entity's
// own table — one query, no N+1 round trip. Unknown entity types and empty
// tenants read nothing (fail closed); SYS_ADMIN reads unscoped.
func fetchEntityAttributes(scope tenantScope, entityType, entityID string, keys []string) map[string]interface{} {
	out := map[string]interface{}{}
	if dbpkg.Pool == nil || len(keys) == 0 || entityID == "" || scope.TenantID == "" {
		return out
	}
	ownership := ""
	if !scope.SysAdmin {
		table, ok := tenantOwnedTable(entityType)
		if !ok {
			return out
		}
		// $3 is bound below, after the key-id array ($2). The table name is a
		// constant from tenantOwnedTable's switch — never user input.
		ownership = " AND EXISTS (SELECT 1 FROM " + table + " o WHERE o.id = $1 AND o.tenant_id = $3)"
	}
	keyIDs := make([]int, 0, len(keys))
	keyByID := map[int]string{}
	for _, k := range keys {
		id := dbpkg.GetOrInsertKeyID(k)
		if id != -1 {
			keyIDs = append(keyIDs, id)
			keyByID[id] = k
		}
	}
	if len(keyIDs) == 0 {
		return out
	}
	args := []interface{}{entityID, pq.Array(keyIDs)}
	if ownership != "" {
		args = append(args, scope.TenantID)
	}
	rows, err := dbpkg.Pool.Query(`
		SELECT attribute_key, last_update_ts, bool_v, str_v, long_v, dbl_v, json_v
		  FROM attribute_kv
		 WHERE entity_id = $1
		   AND attribute_type = 2
		   AND attribute_key = ANY($2)`+ownership, args...)
	if err != nil {
		log.Printf("WARN fetchEntityAttributes %s: %v", entityID, err)
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var keyID int
		var ts int64
		var boolV *bool
		var strV, jsonV *string
		var longV *int64
		var dblV *float64
		if err := rows.Scan(&keyID, &ts, &boolV, &strV, &longV, &dblV, &jsonV); err != nil {
			continue
		}
		k, ok := keyByID[keyID]
		if !ok {
			continue
		}
		out[k] = map[string]interface{}{
			"ts":    ts,
			"value": telemetry.PgValueToTyped(boolV, strV, longV, dblV, jsonV),
		}
	}
	return out
}

func fetchEntityTimeseriesLatest(tenantID, entityType, entityID string, keys []string) map[string]interface{} {
	out := map[string]interface{}{}
	if len(keys) == 0 || entityID == "" {
		return out
	}
	values := map[string]twinstore.Value{}
	if store := twinstore.Global(); store != nil && tenantID != "" {
		if latest, err := store.GetLatestTelemetry(context.Background(), tenantID, entityType, entityID, keys); err == nil {
			values = latest
		}
	}
	for _, key := range keys {
		if value, ok := values[key]; ok {
			out[key] = map[string]interface{}{
				"ts":    value.TS,
				"value": value.Value,
			}
		} else {
			out[key] = map[string]interface{}{"ts": 0, "value": nil}
		}
	}
	return out
}

// buildEntityFieldLatest converts a db row into TB's "latest" ENTITY_FIELD map
// shape: {field: {ts, value}}.
func buildEntityFieldLatest(row map[string]interface{}, fields []struct {
	Type string `json:"type"`
	Key  string `json:"key"`
}) map[string]interface{} {
	now := time.Now().UnixMilli()
	out := map[string]interface{}{}
	for _, f := range fields {
		if f.Type != "ENTITY_FIELD" {
			continue
		}
		var v interface{}
		switch f.Key {
		case "name":
			v = row["name"]
		case "label":
			v = row["label"]
		case "type":
			v = row["type"]
		case "createdTime":
			v = row["createdTime"]
		case "additionalInfo":
			v = "{}"
		default:
			v = ""
		}
		ts := now
		if f.Key == "createdTime" {
			if ct, ok := row["createdTime"].(int64); ok {
				ts = ct
			}
		}
		out[f.Key] = map[string]interface{}{"ts": ts, "value": fmt.Sprintf("%v", v)}
	}
	return out
}

// HandleWebSocket upgrades the HTTP connection and manages the session lifecycle.
func HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade WS: %v", err)
		return
	}

	// Bound the frame size and arm the liveness deadline BEFORE the first read.
	// SetReadLimit makes ReadMessage fail with ErrReadLimit (and send a 1009
	// close frame) instead of allocating whatever the peer announced.
	conn.SetReadLimit(maxWSMessageBytes)
	_ = conn.SetReadDeadline(time.Now().Add(wsPongWait))
	// The pong handler runs on this same goroutine from inside ReadMessage, so
	// refreshing the deadline here needs no extra locking.
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(wsPongWait))
	})

	session := &Session{
		Conn:         conn,
		Subs:         make(map[string][]Subscription),
		AlarmSubs:    make(map[string][]Subscription),
		AttrSubs:     make(map[string][]Subscription),
		EntityCmdMap: make(map[int]EntityRef),
	}

	sessionManager.Lock()
	sessionManager.sessions[session] = true
	sessionManager.Unlock()

	// done stops the keepalive goroutine when the read loop exits, so a closed
	// session never leaves a ticker behind.
	done := make(chan struct{})

	defer func() {
		close(done)
		sessionManager.Lock()
		delete(sessionManager.sessions, session)
		sessionManager.Unlock()
		conn.Close()
		log.Printf("WS session closed. Active sessions: %d", len(sessionManager.sessions))
	}()

	// Keepalive. WriteControl is the one write method gorilla documents as safe
	// to call concurrently with the broadcasters' WriteJSON, so the ping needs
	// no session lock and cannot deadlock against a broadcast in flight.
	go func() {
		t := time.NewTicker(wsPingInterval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(wsWriteWait)); err != nil {
					// Peer is gone or the socket is wedged: closing unblocks
					// the read loop, which runs the cleanup above.
					conn.Close()
					return
				}
			}
		}
	}()

	for {
		messageType, p, err := conn.ReadMessage()
		if err != nil {
			log.Println("WS read error:", err)
			break
		}
		// Any inbound traffic proves the peer is alive — refresh the deadline
		// on reads too, not only on pongs, so a chatty client never trips it.
		_ = conn.SetReadDeadline(time.Now().Add(wsPongWait))
		if messageType == websocket.TextMessage {
			log.Printf("DEBUG WS RAW: %s", string(p))
			var req WsRequest
			_ = json.Unmarshal(p, &req)

			// Deny-by-default WS auth. The TB UI sends a valid authCmd as the
			// FIRST message; a token there is the ONLY way a session becomes
			// authenticated. Previously an invalid/absent token was ignored
			// and the loop kept serving subscriptions anonymously — the WS
			// twin of the HTTP no-auth hole. Now: a present authCmd must carry
			// a valid platform JWT (else we reject and close), and no
			// subscription command is processed until the session is
			// authenticated.
			if req.AuthCmd != nil {
				authed := false
				if tokenVal, ok := (*req.AuthCmd)["token"]; ok {
					if tokenStr, ok := tokenVal.(string); ok {
						// AccessOnly, not ParseAndValidate: a refresh token is
						// signed with the same key and would otherwise open a
						// WS session for its full 7-day TTL.
						if claims, err := authpkg.AccessOnly(tokenStr); err == nil {
							authed = true
							session.mu.Lock()
							session.Authenticated = true
							if tid, ok := claims["tenantId"].(string); ok && tid != "" {
								session.TenantID = tid
							}
							// SYS_ADMIN may read across tenants (mirrors the
							// HTTP telemetry readers). Captured from the same
							// verified claims as the tenant so every later
							// read decision comes from the token, never from
							// a command payload.
							if scopes, ok := claims["scopes"].([]interface{}); ok {
								for _, s := range scopes {
									if str, ok := s.(string); ok && str == "SYS_ADMIN" {
										session.SysAdmin = true
									}
								}
							}
							session.mu.Unlock()
							log.Printf("WS session authenticated tenantId=%s", session.TenantID)
						}
					}
				}
				if !authed {
					log.Printf("WS authCmd rejected: missing or invalid token — closing session")
					_ = conn.WriteJSON(map[string]interface{}{
						"errorCode": http.StatusUnauthorized,
						"errorMsg":  "Authentication failed",
					})
					return // deferred cleanup closes the connection
				}
			}

			// Reject any command that arrives before authentication. The UI
			// always authenticates first, so this only fires for clients that
			// skip authCmd entirely — exactly the anonymous case we must deny.
			session.mu.Lock()
			authenticated := session.Authenticated
			session.mu.Unlock()
			if !authenticated {
				log.Printf("WS command received before authentication — closing session")
				_ = conn.WriteJSON(map[string]interface{}{
					"errorCode": http.StatusUnauthorized,
					"errorMsg":  "Authentication required",
				})
				return // deferred cleanup closes the connection
			}

			// TB v3.7+ unified cmds → classify by Type into legacy arrays
			for _, cmd := range req.Cmds {
				switch cmd.Type {
				case "TIMESERIES":
					req.TsSubCmds = append(req.TsSubCmds, cmd)
				case "ATTRIBUTES":
					req.AttrSubCmds = append(req.AttrSubCmds, cmd)
				case "ALARM_DATA":
					req.AlarmSubCmds = append(req.AlarmSubCmds, cmd)
				case "ALARM_COUNT":
					go handleAlarmCountCmd(session, cmd)
				case "NOTIFICATIONS_COUNT":
					// TB shape: {cmdId, errorCode, errorMsg, totalUnreadCount, sequenceNumber, cmdUpdateType}
					conn.WriteJSON(map[string]interface{}{
						"cmdId":            cmd.CmdId,
						"errorCode":        0,
						"errorMsg":         nil,
						"totalUnreadCount": 0,
						"sequenceNumber":   0,
						"cmdUpdateType":    "NOTIFICATIONS_COUNT",
					})
				case "NOTIFICATIONS":
					// TB returns notifications as a flat array, with cumulative stats.
					conn.WriteJSON(map[string]interface{}{
						"cmdId":            cmd.CmdId,
						"errorCode":        0,
						"errorMsg":         nil,
						"notifications":    []interface{}{},
						"update":           nil,
						"totalUnreadCount": 0,
						"sequenceNumber":   0,
						"cmdUpdateType":    "NOTIFICATIONS",
					})
				case "ENTITY_DATA":
					// Handle entity data (metadata + timeseries for chart widgets)
					go handleEntityDataCmd(session, cmd, p)
				case "ENTITY_COUNT":
					// Home page counter widgets — respond with SQL COUNT
					go handleEntityCountCmd(session, cmd)
				default:
					log.Printf("DEBUG WS cmd type=%s (ignored)", cmd.Type)
				}
			}

			// Legacy: treat latestSubCmds as tsSubCmds
			req.TsSubCmds = append(req.TsSubCmds, req.LatestSubCmds...)

			// Tenant-gate every subscribe BEFORE it is registered. A
			// subscription is not just an initial read: BroadcastTelemetry /
			// BroadcastAttributes / BroadcastAlarmEvent fan out by entityId
			// alone, so a session holding a subscription on another tenant's
			// entity would keep receiving that tenant's live data. Filtering
			// here (outside session.mu — the ownership check hits Postgres)
			// keeps the broadcasters as-is and closes the live-push leak at
			// its only entry point. Unsubscribes are always honoured.
			scope := sessionScope(session)
			req.TsSubCmds = filterOwnedSubCmds(scope, req.TsSubCmds, "telemetry")
			req.AlarmSubCmds = filterOwnedSubCmds(scope, req.AlarmSubCmds, "alarms")
			req.AttrSubCmds = filterOwnedSubCmds(scope, req.AttrSubCmds, "attributes")

			session.mu.Lock()
			for _, cmd := range req.TsSubCmds {
				if cmd.EntityId == "" {
					continue
				}
				if cmd.Unsubscribe {
					session.Subs[cmd.EntityId] = removeSubByCmdId(session.Subs[cmd.EntityId], cmd.CmdId)
					if len(session.Subs[cmd.EntityId]) == 0 {
						delete(session.Subs, cmd.EntityId)
					}
					log.Printf("WS Unsubscribed (telemetry): Entity=%s CmdId=%d", cmd.EntityId, cmd.CmdId)
					continue
				}
				var keys []string
				if cmd.Keys != "" {
					keys = strings.Split(cmd.Keys, ",")
				}
				session.Subs[cmd.EntityId] = append(session.Subs[cmd.EntityId], Subscription{
					CmdId: cmd.CmdId,
					Keys:  keys,
				})
				log.Printf("WS Subscribed (telemetry): Entity=%s CmdId=%d Keys=%v StartTs=%d TimeWindow=%d", cmd.EntityId, cmd.CmdId, keys, cmd.StartTs, cmd.TimeWindow)

				// Send initial historical snapshot from QuestDB
				go sendHistoricalSnapshot(session, cmd)
			}
			for _, cmd := range req.AlarmSubCmds {
				if cmd.EntityId == "" {
					continue
				}
				if cmd.Unsubscribe {
					session.AlarmSubs[cmd.EntityId] = removeSubByCmdId(session.AlarmSubs[cmd.EntityId], cmd.CmdId)
					if len(session.AlarmSubs[cmd.EntityId]) == 0 {
						delete(session.AlarmSubs, cmd.EntityId)
					}
					log.Printf("WS Unsubscribed (alarms): Entity=%s CmdId=%d", cmd.EntityId, cmd.CmdId)
					continue
				}
				session.AlarmSubs[cmd.EntityId] = append(session.AlarmSubs[cmd.EntityId], Subscription{
					CmdId: cmd.CmdId,
				})
				log.Printf("WS Subscribed (alarms): Entity=%s CmdId=%d", cmd.EntityId, cmd.CmdId)
			}
			for _, cmd := range req.AttrSubCmds {
				if cmd.EntityId == "" {
					continue
				}
				if cmd.Unsubscribe {
					session.AttrSubs[cmd.EntityId] = removeSubByCmdId(session.AttrSubs[cmd.EntityId], cmd.CmdId)
					if len(session.AttrSubs[cmd.EntityId]) == 0 {
						delete(session.AttrSubs, cmd.EntityId)
					}
					log.Printf("WS Unsubscribed (attributes): Entity=%s CmdId=%d", cmd.EntityId, cmd.CmdId)
					continue
				}
				var keys []string
				if cmd.Keys != "" {
					keys = strings.Split(cmd.Keys, ",")
				}
				session.AttrSubs[cmd.EntityId] = append(session.AttrSubs[cmd.EntityId], Subscription{
					CmdId: cmd.CmdId,
					Keys:  keys,
				})
				log.Printf("WS Subscribed (attributes): Entity=%s CmdId=%d Scope=%s", cmd.EntityId, cmd.CmdId, cmd.Scope)

				go sendInitialAttributes(session, cmd)
			}
			session.mu.Unlock()
		}
	}
}

// sendHistoricalSnapshot queries QuestDB for historical data and sends it as the initial
// WebSocket payload when a client subscribes. This is what fills the chart on first load.
func sendHistoricalSnapshot(session *Session, cmd WsCmd) {
	if telemetry.PG == nil {
		log.Printf("WARN: Cannot send historical snapshot - QuestDB reader not available")
		return
	}

	// The tenant comes from the session's verified claims, read under the
	// session mutex (this runs on its own goroutine). It is threaded into every
	// store call below, which carry a MANDATORY `AND tenant_id = <tenant>`
	// predicate — an empty tenant therefore reads nothing rather than
	// everything. The subscribe that led here was ownership-gated too.
	scope := sessionScope(session)
	if scope.TenantID == "" {
		log.Printf("WARN: historical snapshot skipped — session has no tenant")
		return
	}

	keys := []string{}
	if cmd.Keys != "" {
		keys = strings.Split(cmd.Keys, ",")
	}

	// Calculate time range
	endTs := time.Now().UnixMilli()
	startTs := cmd.StartTs
	if startTs == 0 && cmd.TimeWindow > 0 {
		startTs = endTs - cmd.TimeWindow
	}
	if startTs == 0 {
		startTs = endTs - 3600000 // default 1 hour
	}

	limit := cmd.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 5000 {
		limit = 5000
	}

	// If no keys specified, discover them from the compact device_telemetry_kv
	// table (tenant-scoped). When still empty, QueryQuestDBKVTimeseries below
	// discovers keys itself, so no separate wide-table column probe is needed.
	if len(keys) == 0 {
		if kvKeys, ok := telemetry.QueryQuestDBKVKeys(scope.TenantID, cmd.EntityId); ok {
			keys = append(keys, kvKeys...)
		}
	}

	// Build the data map in TB format: {"key": [[ts, "value"], [ts, "value"], ...]}.
	// The legacy wide `device_telemetry` snapshot fallback was removed — no
	// pipeline writes that schema, so it was dead, unscoped, raw-`%s` code.
	dataMap := make(map[string]interface{})
	if kvResult, ok := telemetry.QueryQuestDBKVTimeseries(scope.TenantID, cmd.EntityId, keys, startTs, endTs, limit, "ASC", cmd.Agg, fmt.Sprintf("%d", cmd.Interval), false); ok {
		for key, points := range kvResult {
			series := make([][]interface{}, 0, len(points))
			for _, point := range points {
				series = append(series, []interface{}{point["ts"], fmt.Sprintf("%v", point["value"])})
			}
			if len(series) > 0 {
				dataMap[key] = series
			}
		}
	}

	if len(dataMap) == 0 {
		log.Printf("DEBUG: No historical data found for entity=%s keys=%v", cmd.EntityId, keys)
		return
	}

	payload := map[string]interface{}{
		"subscriptionId": cmd.CmdId,
		"data":           dataMap,
	}

	session.mu.Lock()
	err := session.Conn.WriteJSON(payload)
	session.mu.Unlock()

	if err != nil {
		log.Printf("ERROR: Failed to send historical snapshot: %v", err)
	} else {
		log.Printf("WS Historical snapshot sent: entity=%s keys=%d points_per_key~%d", cmd.EntityId, len(dataMap), limit)
	}
}

// handleEntityDataCmd responds to ENTITY_DATA WebSocket commands.
// It handles both metadata-only requests (device page) and timeseries-bearing
// requests (chart widgets with tsCmd/historyCmd) by querying QuestDB.
func handleEntityDataCmd(session *Session, cmd WsCmd, rawMsg []byte) {
	conn := session.Conn
	// Parse the full raw message to extract the query + tsCmd/historyCmd structure
	var fullMsg struct {
		Cmds []struct {
			Type  string `json:"type"`
			CmdId int    `json:"cmdId"`
			Query struct {
				EntityFilter struct {
					Type       string `json:"type"`
					EntityType string `json:"entityType"` // for type=entityType
					DeviceType string `json:"deviceType"` // for type=deviceType (legacy singular)
					AssetType  string `json:"assetType"`  // for type=assetType (legacy singular)
					// TB v4 dashboards send the type filter as an
					// array: deviceTypes: ["Thermostat", ...] or
					// assetTypes: ["building"]. Plus a name filter
					// substring (assetNameFilter / deviceNameFilter).
					DeviceTypes      []string `json:"deviceTypes"`
					AssetTypes       []string `json:"assetTypes"`
					EntityTypes      []string `json:"entityTypes"`
					DeviceNameFilter string   `json:"deviceNameFilter"`
					AssetNameFilter  string   `json:"assetNameFilter"`
					SingleEntity     struct {
						Id         string `json:"id"`
						EntityType string `json:"entityType"`
					} `json:"singleEntity"`
					EntityList     []string `json:"entityList"`               // for type=entityList
					ListEntityType string   `json:"listEntityType,omitempty"` // some clients use this
					// type=relationsQuery — walk Contains/etc. relations
					// from a root entity. The UI substitutes
					// rootStateEntity=true with the dashboard state's
					// active entity (e.g. the building the user clicked
					// on the map) and ships rootEntity populated.
					RootStateEntity      bool    `json:"rootStateEntity"`
					StateEntityParamName *string `json:"stateEntityParamName"`
					RootEntity           struct {
						Id         string `json:"id"`
						EntityType string `json:"entityType"`
					} `json:"rootEntity"`
					Direction string `json:"direction"` // FROM or TO
					MaxLevel  int    `json:"maxLevel"`
					Filters   []struct {
						RelationType string   `json:"relationType"`
						EntityTypes  []string `json:"entityTypes"`
					} `json:"filters"`
				} `json:"entityFilter"`
				PageLink struct {
					PageSize int `json:"pageSize"`
					Page     int `json:"page"`
				} `json:"pageLink"`
				EntityFields []struct {
					Type string `json:"type"`
					Key  string `json:"key"`
				} `json:"entityFields"`
				LatestValues []struct {
					Type string `json:"type"`
					Key  string `json:"key"`
				} `json:"latestValues"`
			} `json:"query"`
			// Chart widgets: realtime subscription
			TsCmd *struct {
				Keys                     []string `json:"keys"`
				StartTs                  int64    `json:"startTs"`
				TimeWindow               int64    `json:"timeWindow"`
				Interval                 int64    `json:"interval"`
				Limit                    int      `json:"limit"`
				Agg                      string   `json:"agg"`
				FetchLatestPreviousPoint bool     `json:"fetchLatestPreviousPoint"`
			} `json:"tsCmd,omitempty"`
			// Chart widgets: historical range
			HistoryCmd *struct {
				Keys     []string `json:"keys"`
				StartTs  int64    `json:"startTs"`
				EndTs    int64    `json:"endTs"`
				Interval int64    `json:"interval"`
				Limit    int      `json:"limit"`
				Agg      string   `json:"agg"`
			} `json:"historyCmd,omitempty"`
			// Latest value command
			LatestCmd *struct {
				Keys []struct {
					Type string `json:"type"`
					Key  string `json:"key"`
				} `json:"keys"`
			} `json:"latestCmd,omitempty"`
		} `json:"cmds"`
	}

	if err := json.Unmarshal(rawMsg, &fullMsg); err != nil {
		log.Printf("ERROR: Failed to parse ENTITY_DATA cmd: %v", err)
		return
	}

	// The session's verified tenant identity gates every read below. Snapshot
	// it once: it never changes after authCmd.
	scope := sessionScope(session)

	// Find the ENTITY_DATA cmd
	for _, c := range fullMsg.Cmds {
		if c.Type != "ENTITY_DATA" {
			continue
		}

		if c.Query.EntityFilter.Type == "apiUsageState" {
			// Resolve the tenant's API_USAGE_STATE row so step 2 (historyCmd) has
			// an entityId to attach timeseries to. Without this the UI's RxJS
			// pipeline iterates over undefined and the home dashboard crashes.
			// The lookup is by tenant, so the cached ref is owned by definition.
			tenantId := scope.TenantID

			var apiStateId string
			if dbpkg.Pool != nil && tenantId != "" {
				_ = dbpkg.Pool.QueryRow(
					"SELECT id FROM api_usage_state WHERE tenant_id = $1 LIMIT 1",
					tenantId,
				).Scan(&apiStateId)
			}

			respData := []interface{}{}
			if apiStateId != "" {
				session.mu.Lock()
				session.EntityCmdMap[c.CmdId] = EntityRef{ID: apiStateId, Type: "API_USAGE_STATE"}
				session.mu.Unlock()

				respData = append(respData, map[string]interface{}{
					"entityId": map[string]interface{}{
						"entityType": "API_USAGE_STATE",
						"id":         apiStateId,
					},
					"latest": map[string]interface{}{
						"ENTITY_FIELD": map[string]interface{}{
							"name":           map[string]interface{}{"ts": time.Now().UnixMilli(), "value": "Tenant"},
							"label":          map[string]interface{}{"ts": 0, "value": ""},
							"additionalInfo": map[string]interface{}{"ts": 0, "value": "{}"},
						},
					},
					"timeseries": map[string]interface{}{},
					"aggLatest":  map[string]interface{}{},
				})
			}

			totalPages := 0
			totalElements := 0
			if len(respData) > 0 {
				totalPages = 1
				totalElements = 1
			}

			session.mu.Lock()
			conn.WriteJSON(map[string]interface{}{
				"cmdId":         c.CmdId,
				"errorCode":     0,
				"errorMsg":      nil,
				"cmdUpdateType": "ENTITY_DATA",
				"data": map[string]interface{}{
					"data":          respData,
					"totalPages":    totalPages,
					"totalElements": totalElements,
					"hasNext":       false,
				},
				"update":          nil,
				"allowedEntities": 10000,
			})
			session.mu.Unlock()
			continue
		}

		// entityType / deviceType / assetType filters: list all matching entities
		filterType := c.Query.EntityFilter.Type
		if filterType == "entityType" || filterType == "deviceType" || filterType == "assetType" {
			// listEntitiesForType is tenant-scoped at the SQL level and returns
			// nothing for an empty tenant, so every id below is already owned.
			tenantId := scope.TenantID

			// Determine the entity type to list
			listEntityType := c.Query.EntityFilter.EntityType
			if filterType == "deviceType" {
				listEntityType = "DEVICE"
			}
			if filterType == "assetType" {
				listEntityType = "ASSET"
			}

			// Type & name filters from the filter object — TB v4
			// dashboards send these as plural arrays plus a name
			// substring; legacy single-string fields fall through.
			var typeFilter []string
			var nameFilter string
			switch filterType {
			case "deviceType":
				typeFilter = c.Query.EntityFilter.DeviceTypes
				if len(typeFilter) == 0 && c.Query.EntityFilter.DeviceType != "" {
					typeFilter = []string{c.Query.EntityFilter.DeviceType}
				}
				nameFilter = c.Query.EntityFilter.DeviceNameFilter
			case "assetType":
				typeFilter = c.Query.EntityFilter.AssetTypes
				if len(typeFilter) == 0 && c.Query.EntityFilter.AssetType != "" {
					typeFilter = []string{c.Query.EntityFilter.AssetType}
				}
				nameFilter = c.Query.EntityFilter.AssetNameFilter
			case "entityType":
				typeFilter = c.Query.EntityFilter.EntityTypes
			}

			pageSize := c.Query.PageLink.PageSize
			if pageSize <= 0 {
				pageSize = 100
			}
			if pageSize > 1024 {
				pageSize = 1024
			}
			page := c.Query.PageLink.Page
			if page < 0 {
				page = 0
			}

			rows := listEntitiesForType(tenantId, listEntityType, typeFilter, nameFilter, pageSize, page*pageSize)

			// Pre-collect attribute keys the dashboard subscribed to.
			// Map widget + entity table both pass them via
			// query.latestValues with type=ATTRIBUTE; without
			// populating latest.ATTRIBUTE, columns and markers stay
			// empty even though the entity list returns correctly.
			attrKeys := []string{}
			tsKeys := []string{}
			for _, lv := range c.Query.LatestValues {
				if lv.Type == "ATTRIBUTE" || lv.Type == "SERVER_SCOPE" || lv.Type == "CLIENT_SCOPE" || lv.Type == "SHARED_SCOPE" {
					attrKeys = append(attrKeys, lv.Key)
				}
				if lv.Type == "TIME_SERIES" {
					tsKeys = append(tsKeys, lv.Key)
				}
			}

			items := []map[string]interface{}{}
			for _, row := range rows {
				entityID := row["id"].(string)
				latest := map[string]interface{}{
					"ENTITY_FIELD": buildEntityFieldLatest(row, c.Query.EntityFields),
				}
				if len(attrKeys) > 0 {
					attrLatest := fetchEntityAttributes(scope, listEntityType, entityID, attrKeys)
					if len(attrLatest) > 0 {
						latest["ATTRIBUTE"] = attrLatest
					}
				}
				if len(tsKeys) > 0 && listEntityType == "DEVICE" {
					latest["TIME_SERIES"] = fetchEntityTimeseriesLatest(tenantId, listEntityType, entityID, tsKeys)
				}
				items = append(items, map[string]interface{}{
					"entityId":   map[string]interface{}{"entityType": listEntityType, "id": entityID},
					"latest":     latest,
					"timeseries": map[string]interface{}{},
					"aggLatest":  map[string]interface{}{},
				})
			}

			session.mu.Lock()
			conn.WriteJSON(map[string]interface{}{
				"cmdId":         c.CmdId,
				"errorCode":     0,
				"errorMsg":      nil,
				"cmdUpdateType": "ENTITY_DATA",
				"data": map[string]interface{}{
					"data":          items,
					"totalPages":    1,
					"totalElements": len(items),
					"hasNext":       false,
				},
				"update":          nil,
				"allowedEntities": 10000,
			})
			session.mu.Unlock()
			log.Printf("WS ENTITY_DATA list type=%s count=%d cmdId=%d", listEntityType, len(items), c.CmdId)
			continue
		}

		// type=relationsQuery — drill-down dashboards (e.g. building
		// detail) walk Contains relations FROM the selected entity to
		// gather floors / zones / devices. The TB UI substitutes
		// rootStateEntity=true with the dashboard state's active
		// entity, so we expect rootEntity.Id to be populated by the
		// time the WS subscription arrives.
		if filterType == "relationsQuery" {
			rootID := c.Query.EntityFilter.RootEntity.Id
			rootType := c.Query.EntityFilter.RootEntity.EntityType
			direction := strings.ToUpper(c.Query.EntityFilter.Direction)
			if direction == "" {
				direction = "FROM"
			}
			maxLevel := c.Query.EntityFilter.MaxLevel
			if maxLevel <= 0 {
				maxLevel = 1
			}
			if maxLevel > 10 {
				maxLevel = 10
			}

			// Allowed entity types + relation types from the filters[].
			allowedTypes := map[string]bool{}
			allowedRels := map[string]bool{}
			for _, f := range c.Query.EntityFilter.Filters {
				if f.RelationType != "" {
					allowedRels[f.RelationType] = true
				}
				for _, et := range f.EntityTypes {
					allowedTypes[et] = true
				}
			}
			if len(allowedRels) == 0 {
				allowedRels["Contains"] = true
			}

			// Pre-collect attribute keys (same as the type-filter branch).
			attrKeys := []string{}
			for _, lv := range c.Query.LatestValues {
				if lv.Type == "ATTRIBUTE" || lv.Type == "SERVER_SCOPE" || lv.Type == "CLIENT_SCOPE" || lv.Type == "SHARED_SCOPE" {
					attrKeys = append(attrKeys, lv.Key)
				}
			}

			items := []map[string]interface{}{}
			if rootID != "" {
				items = walkRelationsQuery(scope, rootID, rootType, direction, maxLevel, allowedRels, allowedTypes, c.Query.EntityFields, attrKeys)
			}

			session.mu.Lock()
			conn.WriteJSON(map[string]interface{}{
				"cmdId":         c.CmdId,
				"errorCode":     0,
				"errorMsg":      nil,
				"cmdUpdateType": "ENTITY_DATA",
				"data": map[string]interface{}{
					"data":          items,
					"totalPages":    1,
					"totalElements": len(items),
					"hasNext":       false,
				},
				"update":          nil,
				"allowedEntities": 10000,
			})
			session.mu.Unlock()
			// Log a short root prefix, not rootID[:8]: rootID is
			// attacker-supplied and a shorter-than-8-char value used to
			// panic this goroutine (process crash — an unauthenticated-shaped
			// DoS from any authenticated session).
			log.Printf("WS ENTITY_DATA relationsQuery root=%s dir=%s lvl=%d count=%d cmdId=%d",
				shortID(rootID), direction, maxLevel, len(items), c.CmdId)
			continue
		}

		entityId := c.Query.EntityFilter.SingleEntity.Id
		entityType := c.Query.EntityFilter.SingleEntity.EntityType

		// Tenant gate for the singleEntity flow. entityId/entityType are taken
		// verbatim from the command payload, and everything downstream —
		// ts_kv_latest, ts_kv, the twin store, the EntityCmdMap registration
		// that makes future BroadcastTelemetry pushes land on this session —
		// keys off them. Prove ownership BEFORE any of that: an unowned entity
		// gets an empty page and no subscription, so neither the initial read
		// nor the live stream can cross tenants. Step 2 of the two-step flow
		// (no entityId, resolved from EntityCmdMap) needs no re-check because
		// only owned refs ever get cached.
		if entityId != "" {
			gateType := entityType
			if gateType == "" {
				gateType = "DEVICE"
			}
			if !scope.allows(gateType, entityId) {
				log.Printf("WS ENTITY_DATA denied: entity=%s type=%s not owned by tenant=%q cmdId=%d",
					entityId, gateType, scope.TenantID, c.CmdId)
				session.mu.Lock()
				conn.WriteJSON(map[string]interface{}{
					"cmdId":         c.CmdId,
					"errorCode":     0,
					"errorMsg":      nil,
					"cmdUpdateType": "ENTITY_DATA",
					"data": map[string]interface{}{
						"data":          []interface{}{},
						"totalPages":    0,
						"totalElements": 0,
						"hasNext":       false,
					},
					"update":          nil,
					"allowedEntities": 10000,
				})
				session.mu.Unlock()
				continue
			}
		}

		// Collect the live-update key set the cmd subscribed to. We
		// merge keys from query.latestValues (gauge/value widgets),
		// tsCmd.keys (chart realtime), latestCmd.keys (latest panel)
		// and historyCmd.keys (chart historical). Broadcasts later
		// filter to this set so we never push a key the widget didn't
		// register, which would crash the UI's dataKeys lookup.
		subKeys := map[string]struct{}{}
		for _, lv := range c.Query.LatestValues {
			if lv.Key != "" {
				subKeys[lv.Key] = struct{}{}
			}
		}
		if c.TsCmd != nil {
			for _, k := range c.TsCmd.Keys {
				subKeys[k] = struct{}{}
			}
		}
		if c.HistoryCmd != nil {
			for _, k := range c.HistoryCmd.Keys {
				subKeys[k] = struct{}{}
			}
		}
		if c.LatestCmd != nil {
			for _, k := range c.LatestCmd.Keys {
				if k.Key != "" {
					subKeys[k.Key] = struct{}{}
				}
			}
		}
		keysList := make([]string, 0, len(subKeys))
		for k := range subKeys {
			keysList = append(keysList, k)
		}

		// Two-step ENTITY_DATA flow:
		// Step 1: UI sends ENTITY_DATA with query (has entityId) → cache the ref
		// Step 2: UI sends ENTITY_DATA with tsCmd only (no query) → resolve from cache
		session.mu.Lock()
		if entityId != "" {
			if entityType == "" {
				entityType = "DEVICE"
			}
			// Merge keys with any prior subscription on the same cmd
			// (step 2 of the two-step flow lands tsCmd keys after the
			// query already cached the entity ref).
			if existing, ok := session.EntityCmdMap[c.CmdId]; ok {
				for _, k := range existing.Keys {
					if _, dup := subKeys[k]; !dup {
						keysList = append(keysList, k)
					}
				}
			}
			session.EntityCmdMap[c.CmdId] = EntityRef{ID: entityId, Type: entityType, Keys: keysList}
		} else if ref, ok := session.EntityCmdMap[c.CmdId]; ok {
			entityId = ref.ID
			entityType = ref.Type
			// Step 2: keep ref but extend its key set with the new
			// keys from this message (ts/historyCmd often arrives
			// separately from the query).
			if len(keysList) > 0 {
				existing := map[string]struct{}{}
				for _, k := range ref.Keys {
					existing[k] = struct{}{}
				}
				for _, k := range keysList {
					existing[k] = struct{}{}
				}
				merged := make([]string, 0, len(existing))
				for k := range existing {
					merged = append(merged, k)
				}
				ref.Keys = merged
				session.EntityCmdMap[c.CmdId] = ref
			}
		}
		if entityType == "" {
			entityType = "DEVICE"
		}
		session.mu.Unlock()

		if entityId == "" {
			log.Printf("WARN: ENTITY_DATA cmd=%d has no entityId and none cached", c.CmdId)
			continue
		}

		// Build entity fields response
		entityFields := map[string]map[string]interface{}{}
		for _, f := range c.Query.EntityFields {
			switch f.Key {
			case "name":
				entityFields["name"] = map[string]interface{}{"key": "name", "value": entityId}
			case "label":
				entityFields["label"] = map[string]interface{}{"key": "label", "value": ""}
			case "additionalInfo":
				entityFields["additionalInfo"] = map[string]interface{}{"key": "additionalInfo", "value": "{}"}
			case "createdTime":
				entityFields["createdTime"] = map[string]interface{}{"key": "createdTime", "value": time.Now().UnixMilli()}
			default:
				entityFields[f.Key] = map[string]interface{}{"key": f.Key, "value": ""}
			}
		}

		// Build latest values — DEVICE telemetry comes from QuestDB; everything
		// else (api_usage_state, asset, ...) comes from Postgres ts_kv_latest.
		// Values are emitted with their native JSON type. The TB UI v3.7+
		// uses ENTITY_DATA subscriptions in strict-type mode by default —
		// stringifying numbers/booleans makes the UI silently reject the
		// row and the "Latest telemetry" tab stays empty even though the
		// data is in the store.
		latestMap := map[string]interface{}{}
		tsLatest := map[string]interface{}{}
		sessionTenantID := scope.TenantID
		for _, lv := range c.Query.LatestValues {
			if lv.Type == "TIME_SERIES" {
				var found bool
				if store := twinstore.Global(); store != nil && sessionTenantID != "" {
					if values, err := store.GetLatestTelemetry(context.Background(), sessionTenantID, entityType, entityId, []string{lv.Key}); err == nil {
						if value, ok := values[lv.Key]; ok {
							tsLatest[lv.Key] = map[string]interface{}{
								"ts":    value.TS,
								"value": value.Value,
							}
							found = true
						}
					}
				}
				if !found && natsTwinStateAuthoritative(entityType) {
					tsLatest[lv.Key] = map[string]interface{}{"ts": 0, "value": nil}
					continue
				}
				// DEVICE latest from the compact device_telemetry_kv table
				// (tenant-scoped by DeviceKVLatest's mandatory tenant_id predicate).
				// The legacy wide `device_telemetry` reader was removed: no pipeline
				// writes that schema, and it interpolated entityId raw with no tenant
				// filter (IDOR). sessionTenantID == "" (unauthenticated) falls through
				// to the not-found handling rather than issuing an unscoped query.
				if !found && strings.EqualFold(entityType, "DEVICE") && telemetry.PG != nil && sessionTenantID != "" {
					if ts, value, ok := telemetry.DeviceKVLatest(sessionTenantID, entityId, lv.Key, false); ok {
						tsLatest[lv.Key] = map[string]interface{}{
							"ts":    ts,
							"value": value,
						}
						found = true
					}
				}
				// Non-device entity latest from GreptimeDB entity_telemetry_kv when the
				// read backend is flipped. sessionTenantID is resolved here, so it is
				// passed through non-empty — the rendered last-point query carries a
				// MANDATORY `AND tenant_id = <session tenant>` predicate (two-layer
				// isolation). If the session is unauthenticated (sessionTenantID == "")
				// we fall through to the not-found/empty handling rather than issuing an
				// unscoped query. Default postgres ⇒ the unchanged Postgres block below.
				if !found && telemetry.UsageReadBackend() == "greptime" && telemetry.PG != nil &&
					!strings.EqualFold(entityType, "DEVICE") && sessionTenantID != "" {
					if ts, value, ok := telemetry.EntityKVLatest(entityType, entityId, sessionTenantID, lv.Key); ok {
						tsLatest[lv.Key] = map[string]interface{}{
							"ts":    ts,
							"value": value,
						}
						found = true
					}
				}
				// Postgres ts_kv_latest is keyed by entity_id and has NO tenant
				// column, so the tenant boundary is the entity's own owner. The
				// singleEntity gate above already proved entityId belongs to
				// scope; the sessionTenantID != "" guard is the fail-closed
				// backstop so a tenant-less session can never reach this read.
				if !found && telemetry.UsageReadBackend() != "greptime" && dbpkg.Pool != nil && sessionTenantID != "" {
					keyId := dbpkg.GetOrInsertKeyID(lv.Key)
					if keyId != -1 {
						var ts int64
						var boolV *bool
						var strV, jsonV *string
						var longV *int64
						var dblV *float64
						err := dbpkg.Pool.QueryRow(
							`SELECT ts, bool_v, str_v, long_v, dbl_v, json_v FROM ts_kv_latest
							 WHERE entity_id = $1 AND key = $2`,
							entityId, keyId,
						).Scan(&ts, &boolV, &strV, &longV, &dblV, &jsonV)
						if err == nil {
							tsLatest[lv.Key] = map[string]interface{}{
								"ts":    ts,
								"value": telemetry.PgValueToTyped(boolV, strV, longV, dblV, jsonV),
							}
							found = true
						}
					}
				}
				if !found {
					tsLatest[lv.Key] = map[string]interface{}{"ts": 0, "value": nil}
				}
			} else if lv.Type == "ATTRIBUTE" || lv.Type == "SERVER_SCOPE" || lv.Type == "CLIENT_SCOPE" || lv.Type == "SHARED_SCOPE" {
				if latestMap[lv.Type] == nil {
					latestMap[lv.Type] = map[string]interface{}{}
				}
				latestMap[lv.Type].(map[string]interface{})[lv.Key] = map[string]interface{}{"ts": 0, "value": ""}
			}
		}

		// Merge latest maps
		latestResult := map[string]interface{}{
			"ENTITY_FIELD": entityFields,
		}
		if len(tsLatest) > 0 {
			latestResult["TIME_SERIES"] = tsLatest
		}
		for k, v := range latestMap {
			latestResult[k] = v
		}

		// Build timeseries data for tsCmd or historyCmd. DEVICE telemetry comes
		// from QuestDB; everything else (api_usage_state, asset, ...) lives in
		// Postgres ts_kv.
		timeseriesData := map[string]interface{}{}
		if c.TsCmd != nil || c.HistoryCmd != nil {
			var keys []string
			var startTs, endTs int64
			var limit int
			var agg string
			var interval int64

			if c.TsCmd != nil {
				keys = c.TsCmd.Keys
				endTs = time.Now().UnixMilli()
				startTs = c.TsCmd.StartTs
				if startTs == 0 && c.TsCmd.TimeWindow > 0 {
					startTs = endTs - c.TsCmd.TimeWindow
				}
				limit = c.TsCmd.Limit
				agg = c.TsCmd.Agg
				interval = c.TsCmd.Interval
				log.Printf("DEBUG: ENTITY_DATA tsCmd entityType=%s keys=%v startTs=%d timeWindow=%d", entityType, keys, startTs, c.TsCmd.TimeWindow)
			} else if c.HistoryCmd != nil {
				keys = c.HistoryCmd.Keys
				startTs = c.HistoryCmd.StartTs
				endTs = c.HistoryCmd.EndTs
				limit = c.HistoryCmd.Limit
				agg = c.HistoryCmd.Agg
				interval = c.HistoryCmd.Interval
				log.Printf("DEBUG: ENTITY_DATA historyCmd entityType=%s keys=%v startTs=%d endTs=%d", entityType, keys, startTs, endTs)
			}

			if limit <= 0 {
				limit = 500
			}
			if limit > 5000 {
				limit = 5000
			}
			if startTs == 0 {
				startTs = time.Now().UnixMilli() - 3600000
			}
			if endTs == 0 {
				endTs = time.Now().UnixMilli()
			}

			useQuest := strings.EqualFold(entityType, "DEVICE") && telemetry.PG != nil
			if useQuest {
				if kvResult, ok := telemetry.QueryQuestDBKVTimeseries(scope.TenantID, entityId, keys, startTs, endTs, limit, "ASC", agg, fmt.Sprintf("%d", interval), false); ok {
					for key, points := range kvResult {
						if len(points) > 0 {
							timeseriesData[key] = points
						}
					}
				}
			}
			// DEVICE timeseries come from the tenant-scoped device_telemetry_kv KV
			// query above. The legacy wide `device_telemetry` fallback loop was
			// removed (dead schema, raw-`%s` IDOR). Non-DEVICE entities read from
			// Postgres ts_kv below.
			if !useQuest && dbpkg.Pool != nil && sessionTenantID != "" {
				// Read from Postgres ts_kv for non-device entities (api_usage_state, asset, ...).
				// ts_kv has no tenant column: isolation comes from the
				// singleEntity ownership gate above (entityId is proven to
				// belong to the session's tenant before we get here), plus this
				// fail-closed guard for a tenant-less session.
				for _, key := range keys {
					keyId := dbpkg.GetOrInsertKeyID(key)
					if keyId == -1 {
						continue
					}
					rows, err := dbpkg.Pool.Query(
						`SELECT ts, bool_v, str_v, long_v, dbl_v, json_v FROM ts_kv
						 WHERE entity_id = $1 AND key = $2 AND ts >= $3 AND ts <= $4
						 ORDER BY ts ASC LIMIT $5`,
						entityId, keyId, startTs, endTs, limit,
					)
					if err != nil {
						log.Printf("WARN ts_kv query for entity=%s key=%s: %v", entityId, key, err)
						continue
					}
					var points []map[string]interface{}
					for rows.Next() {
						var ts int64
						var boolV *bool
						var strV, jsonV *string
						var longV *int64
						var dblV *float64
						if err := rows.Scan(&ts, &boolV, &strV, &longV, &dblV, &jsonV); err != nil {
							continue
						}
						points = append(points, map[string]interface{}{
							"ts":    ts,
							"value": telemetry.PgValueToString(boolV, strV, longV, dblV, jsonV),
						})
					}
					rows.Close()
					if len(points) > 0 {
						timeseriesData[key] = points
					}
				}
			}
			log.Printf("DEBUG: ENTITY_DATA timeseries filled: entity=%s entityType=%s keys=%d (questdb=%v)",
				entityId, entityType, len(timeseriesData), useQuest)
		}

		entityData := map[string]interface{}{
			"entityId": map[string]interface{}{
				"id":         entityId,
				"entityType": entityType,
			},
			"entityFields": entityFields,
			"latest":       latestResult,
			"timeseries":   timeseriesData,
		}

		// Build the response based on whether this is an initial query (step 1)
		// or a tsCmd/historyCmd update (step 2)
		var response map[string]interface{}
		if c.TsCmd != nil || c.HistoryCmd != nil {
			// Step 2: timeseries update — send as 'update' array
			response = map[string]interface{}{
				"cmdId":           c.CmdId,
				"errorCode":       0,
				"errorMsg":        nil,
				"cmdUpdateType":   "ENTITY_DATA",
				"update":          []interface{}{entityData},
				"allowedEntities": 10000,
			}
		} else {
			// Step 1: initial entity data — send as 'data' PageData
			response = map[string]interface{}{
				"cmdId":         c.CmdId,
				"errorCode":     0,
				"errorMsg":      nil,
				"cmdUpdateType": "ENTITY_DATA",
				"data": map[string]interface{}{
					"data":          []interface{}{entityData},
					"totalPages":    1,
					"totalElements": 1,
					"hasNext":       false,
				},
				"update":          nil,
				"allowedEntities": 10000,
			}
		}

		session.mu.Lock()
		err := conn.WriteJSON(response)
		session.mu.Unlock()

		if err != nil {
			log.Printf("ERROR: Failed to send ENTITY_DATA response: %v", err)
		} else {
			log.Printf("WS ENTITY_DATA response sent: entity=%s cmdId=%d tsKeys=%d isUpdate=%v", entityId, c.CmdId, len(timeseriesData), c.TsCmd != nil || c.HistoryCmd != nil)
		}
	}
}

func natsTwinStateAuthoritative(entityType string) bool {
	return strings.EqualFold(entityType, "DEVICE") && strings.EqualFold(strings.TrimSpace(os.Getenv("TWIN_STATE_STORE")), "nats")
}

// handleEntityCountCmd responds to WS ENTITY_COUNT commands used by home-page counter widgets.
// It queries PostgreSQL for the entity count and sends the result back over the WebSocket.
func handleEntityCountCmd(session *Session, cmd WsCmd) {
	// Every count below is either `WHERE tenant_id = $1` or a relations walk
	// rooted at an ownership-checked entity. A tenant-less session must count
	// nothing rather than fall through to an unscoped/erroring query.
	scope := sessionScope(session)
	tenantId := scope.TenantID
	if dbpkg.Pool == nil || tenantId == "" {
		log.Printf("WARN: handleEntityCountCmd without tenant or db — cmdId=%d", cmd.CmdId)
		return
	}

	// Parse the query supporting both legacy single-type filters and the
	// v4 dashboard variants: entityType list, asset/device type arrays,
	// AND relationsQuery (with rootEntity already substituted by the UI
	// from rootStateEntity).
	var query struct {
		EntityFilter struct {
			Type         string   `json:"type"`
			EntityType   string   `json:"entityType"`
			DeviceType   string   `json:"deviceType"`
			AssetType    string   `json:"assetType"`
			DeviceTypes  []string `json:"deviceTypes"`
			AssetTypes   []string `json:"assetTypes"`
			EntityTypes  []string `json:"entityTypes"`
			SingleEntity struct {
				Id         string `json:"id"`
				EntityType string `json:"entityType"`
			} `json:"singleEntity"`
			// relationsQuery shape (mirrors the ENTITY_DATA branch).
			RootStateEntity bool `json:"rootStateEntity"`
			RootEntity      struct {
				Id         string `json:"id"`
				EntityType string `json:"entityType"`
			} `json:"rootEntity"`
			Direction string `json:"direction"`
			MaxLevel  int    `json:"maxLevel"`
			Filters   []struct {
				RelationType string   `json:"relationType"`
				EntityTypes  []string `json:"entityTypes"`
			} `json:"filters"`
		} `json:"entityFilter"`
		KeyFilters []struct {
			Key struct {
				Type string `json:"type"`
				Key  string `json:"key"`
			} `json:"key"`
			ValueType string `json:"valueType"`
			Predicate struct {
				Operation string `json:"operation"`
				Value     struct {
					DefaultValue interface{} `json:"defaultValue"`
				} `json:"value"`
				Type string `json:"type"`
			} `json:"predicate"`
		} `json:"keyFilters"`
	}
	if raw, err := json.Marshal(cmd.Query); err == nil {
		_ = json.Unmarshal(raw, &query)
	}

	count := 0
	filterType := query.EntityFilter.Type

	switch filterType {
	case "relationsQuery":
		// Walk the relations graph and count the matching entities.
		// Mirror the handleEntityDataCmd branch so dashboard counts on
		// the same alias agree with table rows.
		rootID := query.EntityFilter.RootEntity.Id
		rootType := query.EntityFilter.RootEntity.EntityType
		direction := strings.ToUpper(query.EntityFilter.Direction)
		if direction == "" {
			direction = "FROM"
		}
		maxLevel := query.EntityFilter.MaxLevel
		if maxLevel <= 0 {
			maxLevel = 1
		}
		if maxLevel > 10 {
			maxLevel = 10
		}
		allowedRels := map[string]bool{}
		allowedTypes := map[string]bool{}
		for _, f := range query.EntityFilter.Filters {
			if f.RelationType != "" {
				allowedRels[f.RelationType] = true
			}
			for _, et := range f.EntityTypes {
				allowedTypes[et] = true
			}
		}
		if len(allowedRels) == 0 {
			allowedRels["Contains"] = true
		}
		if rootID != "" {
			items := walkRelationsQuery(scope, rootID, rootType, direction, maxLevel,
				allowedRels, allowedTypes,
				[]struct {
					Type string `json:"type"`
					Key  string `json:"key"`
				}{}, nil)
			count = len(items)
		}
	case "assetType":
		// Filter assets by tenant + assetTypes array (or singular fallback).
		typeFilter := query.EntityFilter.AssetTypes
		if len(typeFilter) == 0 && query.EntityFilter.AssetType != "" {
			typeFilter = []string{query.EntityFilter.AssetType}
		}
		if len(typeFilter) > 0 {
			dbpkg.Pool.QueryRow(
				"SELECT count(*) FROM asset WHERE tenant_id = $1 AND type = ANY($2)",
				tenantId, pq.Array(typeFilter)).Scan(&count)
		} else {
			dbpkg.Pool.QueryRow("SELECT count(*) FROM asset WHERE tenant_id = $1", tenantId).Scan(&count)
		}
	case "deviceType":
		typeFilter := query.EntityFilter.DeviceTypes
		if len(typeFilter) == 0 && query.EntityFilter.DeviceType != "" {
			typeFilter = []string{query.EntityFilter.DeviceType}
		}
		if len(typeFilter) > 0 {
			dbpkg.Pool.QueryRow(
				"SELECT count(*) FROM device WHERE tenant_id = $1 AND type = ANY($2)",
				tenantId, pq.Array(typeFilter)).Scan(&count)
		} else {
			dbpkg.Pool.QueryRow("SELECT count(*) FROM device WHERE tenant_id = $1", tenantId).Scan(&count)
		}
	default:
		// Legacy entityType behaviour.
		entityType := strings.ToLower(query.EntityFilter.EntityType)
		switch entityType {
		case "device":
			if len(query.KeyFilters) > 0 {
				for _, kf := range query.KeyFilters {
					if kf.Key.Key == "active" && kf.Predicate.Type == "BOOLEAN" {
						val := kf.Predicate.Value.DefaultValue
						if want, ok := val.(bool); ok {
							rows, err := dbpkg.Pool.Query("SELECT id FROM device WHERE tenant_id = $1", tenantId)
							if err == nil {
								hotCount := 0
								hotSeen := 0
								for rows.Next() {
									var deviceID string
									if err := rows.Scan(&deviceID); err != nil {
										continue
									}
									if active, ok := deviceactivity.ActiveFromTwin(context.Background(), tenantId, deviceID, time.Now()); ok {
										hotSeen++
										if active == want {
											hotCount++
										}
									} else if !want {
										hotCount++
									}
								}
								rows.Close()
								if hotSeen > 0 {
									count = hotCount
									break
								}
							}
						}
						dbpkg.Pool.QueryRow(`SELECT count(DISTINCT d.id) FROM device d
							JOIN attribute_kv a ON a.entity_id = d.id
							JOIN key_dictionary k ON k.key_id = a.attribute_key
							WHERE d.tenant_id = $1 AND k.key = 'active' AND a.bool_v = $2`, tenantId, val).Scan(&count)
					}
				}
			} else {
				dbpkg.Pool.QueryRow("SELECT count(*) FROM device WHERE tenant_id = $1", tenantId).Scan(&count)
			}
		case "asset":
			dbpkg.Pool.QueryRow("SELECT count(*) FROM asset WHERE tenant_id = $1", tenantId).Scan(&count)
		case "customer":
			dbpkg.Pool.QueryRow("SELECT count(*) FROM customer WHERE tenant_id = $1 AND title != 'Public'", tenantId).Scan(&count)
		case "dashboard":
			dbpkg.Pool.QueryRow("SELECT count(*) FROM dashboard WHERE tenant_id = $1", tenantId).Scan(&count)
		case "user":
			dbpkg.Pool.QueryRow("SELECT count(*) FROM tb_user WHERE tenant_id = $1", tenantId).Scan(&count)
		}
	}

	session.mu.Lock()
	session.Conn.WriteJSON(map[string]interface{}{
		"cmdId":         cmd.CmdId,
		"count":         count,
		"errorCode":     0,
		"errorMsg":      nil,
		"cmdUpdateType": "COUNT_DATA",
	})
	session.mu.Unlock()
	log.Printf("WS ENTITY_COUNT: filter=%s count=%d cmdId=%d", filterType, count, cmd.CmdId)
}

func handleAlarmCountCmd(session *Session, cmd WsCmd) {
	if dbpkg.Pool == nil {
		return
	}
	session.mu.Lock()
	tenantId := session.TenantID
	session.mu.Unlock()

	if tenantId == "" {
		log.Printf("WARN: handleAlarmCountCmd missing tenantId")
		return
	}

	var query struct {
		StatusList   []string `json:"statusList"`
		SeverityList []string `json:"severityList"`
	}
	if raw, err := json.Marshal(cmd.Query); err == nil {
		json.Unmarshal(raw, &query)
	}

	var count int

	// Build the SQL query
	sqlQuery := "SELECT count(*) FROM alarm WHERE tenant_id = $1"
	args := []interface{}{tenantId}
	argIdx := 2

	if len(query.StatusList) > 0 {
		var statusConds []string
		for _, s := range query.StatusList {
			switch s {
			case "ACTIVE":
				statusConds = append(statusConds, "cleared = false")
			case "CLEARED":
				statusConds = append(statusConds, "cleared = true")
			case "ACK":
				statusConds = append(statusConds, "acknowledged = true")
			case "UNACK":
				statusConds = append(statusConds, "acknowledged = false")
			case "ACTIVE_UNACK":
				statusConds = append(statusConds, "(cleared = false AND acknowledged = false)")
			case "ACTIVE_ACK":
				statusConds = append(statusConds, "(cleared = false AND acknowledged = true)")
			case "CLEARED_UNACK":
				statusConds = append(statusConds, "(cleared = true AND acknowledged = false)")
			case "CLEARED_ACK":
				statusConds = append(statusConds, "(cleared = true AND acknowledged = true)")
			}
		}
		if len(statusConds) > 0 {
			sqlQuery += " AND (" + strings.Join(statusConds, " OR ") + ")"
		}
	}

	if len(query.SeverityList) > 0 {
		var sevPlaceholders []string
		for _, sev := range query.SeverityList {
			sevPlaceholders = append(sevPlaceholders, fmt.Sprintf("$%d", argIdx))
			args = append(args, sev)
			argIdx++
		}
		sqlQuery += " AND severity IN (" + strings.Join(sevPlaceholders, ",") + ")"
	}

	err := dbpkg.Pool.QueryRow(sqlQuery, args...).Scan(&count)
	if err != nil {
		log.Printf("ERROR: handleAlarmCountCmd query failed: %v", err)
		return
	}

	session.mu.Lock()
	session.Conn.WriteJSON(map[string]interface{}{
		"cmdId":         cmd.CmdId,
		"count":         count,
		"errorCode":     0,
		"errorMsg":      nil,
		"cmdUpdateType": "ALARM_COUNT_DATA",
	})
	session.mu.Unlock()
	log.Printf("WS ALARM_COUNT: count=%d cmdId=%d", count, cmd.CmdId)
}

// BroadcastAttributes sends an attribute update to all sessions subscribed to attributes for the given entity.
func BroadcastAttributes(entityId string, scope string, data map[string]interface{}) {
	sessionManager.RLock()
	defer sessionManager.RUnlock()

	ts := time.Now().UnixMilli()
	for session := range sessionManager.sessions {
		session.mu.Lock()
		subs, ok := session.AttrSubs[entityId]
		if ok {
			for _, sub := range subs {
				payload := map[string]interface{}{
					"subscriptionId": sub.CmdId,
					"data":           make(map[string]interface{}),
				}
				dataMap := payload["data"].(map[string]interface{})
				for k, v := range data {
					if len(sub.Keys) == 0 || contains(sub.Keys, k) {
						strVal := ""
						switch v := v.(type) {
						case string:
							strVal = v
						default:
							b, _ := json.Marshal(v)
							strVal = string(b)
						}
						dataMap[k] = [][]interface{}{{ts, strVal}}
					}
				}
				if len(dataMap) > 0 {
					if err := session.Conn.WriteJSON(payload); err != nil {
						log.Printf("Error sending WS attributes payload: %v", err)
					}
				}
			}
		}
		session.mu.Unlock()
	}
}

// sendInitialAttributes queries PostgreSQL for the latest attributes and sends them
// as the initial WebSocket payload when a client subscribes.
func sendInitialAttributes(session *Session, cmd WsCmd) {
	if dbpkg.Pool == nil {
		return
	}

	// attribute_kv is keyed by entity_id with no tenant column, so the read is
	// scoped by an EXISTS ownership predicate on the entity's own table. The
	// subscribe that led here was gated the same way; this makes the READ
	// itself tenant-scoped rather than trusting the caller. Empty tenant or an
	// entity type we cannot prove ownership for ⇒ read nothing.
	scope := sessionScope(session)
	if scope.TenantID == "" {
		log.Printf("WARN: sendInitialAttributes skipped — session has no tenant")
		return
	}
	entityType := cmd.EntityType
	if entityType == "" {
		entityType = "DEVICE"
	}
	ownership := ""
	if !scope.SysAdmin {
		table, ok := tenantOwnedTable(entityType)
		if !ok {
			log.Printf("WARN: sendInitialAttributes denied — unknown entity type %q", entityType)
			return
		}
		// Table name is a constant from tenantOwnedTable; ids stay bound.
		ownership = " AND EXISTS (SELECT 1 FROM " + table + " o WHERE o.id = a.entity_id AND o.tenant_id = $2)"
	}

	keys := []string{}
	if cmd.Keys != "" {
		keys = strings.Split(cmd.Keys, ",")
	}

	query := `SELECT k.key, a.bool_v, a.str_v, a.long_v, a.dbl_v, a.json_v, a.last_update_ts
		FROM attribute_kv a
		JOIN key_dictionary k ON a.attribute_key = k.key_id
		WHERE a.entity_id = $1`
	args := []interface{}{cmd.EntityId}
	argIdx := 2
	if ownership != "" {
		query += ownership
		args = append(args, scope.TenantID)
		argIdx++
	}

	if cmd.Scope != "" && cmd.Scope != "ANY_SCOPE" {
		attrType := -1
		switch strings.ToUpper(cmd.Scope) {
		case "CLIENT_SCOPE":
			attrType = 0
		case "SHARED_SCOPE":
			attrType = 1
		case "SERVER_SCOPE":
			attrType = 2
		}
		if attrType != -1 {
			query += fmt.Sprintf(" AND a.attribute_type = $%d", argIdx)
			args = append(args, attrType)
			argIdx++
		}
	}

	if len(keys) > 0 {
		placeholders := make([]string, len(keys))
		for i, k := range keys {
			placeholders[i] = fmt.Sprintf("$%d", argIdx+i)
			args = append(args, k)
			argIdx++
		}
		query += " AND k.key IN (" + strings.Join(placeholders, ",") + ")"
	}

	rows, err := dbpkg.Pool.Query(query, args...)
	if err != nil {
		log.Printf("WARN: sendInitialAttributes query failed: %v", err)
		return
	}
	defer rows.Close()

	dataMap := make(map[string]interface{})
	for rows.Next() {
		var key string
		var boolV *bool
		var strV *string
		var longV *int64
		var dblV *float64
		var jsonV *string
		var lastTs int64

		if err := rows.Scan(&key, &boolV, &strV, &longV, &dblV, &jsonV, &lastTs); err != nil {
			continue
		}

		val := telemetry.PgValueToString(boolV, strV, longV, dblV, jsonV)
		dataMap[key] = [][]interface{}{{lastTs, val}}
	}

	payload := map[string]interface{}{
		"subscriptionId": cmd.CmdId,
		"data":           dataMap,
	}

	session.mu.Lock()
	err = session.Conn.WriteJSON(payload)
	session.mu.Unlock()

	if err != nil {
		log.Printf("ERROR: Failed to send initial attributes: %v", err)
	} else {
		log.Printf("WS Initial attributes sent: entity=%s keys=%d", cmd.EntityId, len(dataMap))
	}
}
