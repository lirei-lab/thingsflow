package twinstore

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

const Schema = "thingsflow.twin-state.v1"

var ErrNotFound = errors.New("twin state not found")

type Value struct {
	TS    int64       `json:"ts"`
	Value interface{} `json:"value"`
}

type State struct {
	Schema     string                      `json:"schema"`
	TenantID   string                      `json:"tenantId"`
	EntityType string                      `json:"entityType"`
	EntityID   string                      `json:"entityId"`
	UpdatedTS  int64                       `json:"updatedTs"`
	Telemetry  map[string]Value            `json:"telemetry"`
	Attributes map[string]map[string]Value `json:"attributes"`
	Activity   map[string]interface{}      `json:"activity"`
}

type Change struct {
	Old State
	New State
}

type Store interface {
	GetEntityState(ctx context.Context, tenantID, entityType, entityID string) (State, error)
	GetTelemetryKeys(ctx context.Context, tenantID, entityType, entityID string) ([]string, error)
	GetLatestTelemetry(ctx context.Context, tenantID, entityType, entityID string, keys []string) (map[string]Value, error)
	MergeTelemetry(ctx context.Context, tenantID, entityType, entityID string, ts int64, values map[string]interface{}) error
	MergeAttributes(ctx context.Context, tenantID, entityType, entityID, scope string, ts int64, values map[string]interface{}) error
	Watch(ctx context.Context, prefix string) (<-chan Change, error)
}

var global Store

func SetGlobal(store Store) {
	global = store
}

func Global() Store {
	return global
}

func NormalizeEntityType(entityType string) string {
	entityType = strings.TrimSpace(strings.ToUpper(entityType))
	if entityType == "" {
		return "DEVICE"
	}
	return entityType
}

func Key(entityType, tenantID, entityID string) string {
	return NormalizeEntityType(entityType) + "." + tenantID + "." + entityID
}

func TelemetryKey(entityType, tenantID, entityID, telemetryKey string) string {
	return TelemetryPrefix(entityType, tenantID, entityID) + strings.TrimSpace(telemetryKey)
}

func TelemetryPrefix(entityType, tenantID, entityID string) string {
	return Key(entityType, tenantID, entityID) + ".telemetry."
}

func ParseTelemetryKey(key string) (entityType, tenantID, entityID, telemetryKey string, ok bool) {
	parts := strings.SplitN(strings.TrimSpace(key), ".", 5)
	if len(parts) != 5 || parts[3] != "telemetry" || parts[4] == "" {
		return "", "", "", "", false
	}
	return NormalizeEntityType(parts[0]), parts[1], parts[2], parts[4], true
}

type MemoryStore struct {
	mu      sync.RWMutex
	states  map[string]State
	watches []memoryWatch
}

type memoryWatch struct {
	prefix string
	ch     chan Change
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{states: map[string]State{}}
}

func (s *MemoryStore) GetEntityState(ctx context.Context, tenantID, entityType, entityID string) (State, error) {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, ok := s.states[Key(entityType, tenantID, entityID)]
	if !ok {
		return State{}, ErrNotFound
	}
	return cloneState(state), nil
}

func (s *MemoryStore) GetTelemetryKeys(ctx context.Context, tenantID, entityType, entityID string) ([]string, error) {
	state, err := s.GetEntityState(ctx, tenantID, entityType, entityID)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(state.Telemetry))
	for key := range state.Telemetry {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *MemoryStore) GetLatestTelemetry(ctx context.Context, tenantID, entityType, entityID string, keys []string) (map[string]Value, error) {
	state, err := s.GetEntityState(ctx, tenantID, entityType, entityID)
	if err != nil {
		return nil, err
	}
	result := map[string]Value{}
	if len(keys) == 0 {
		for key, value := range state.Telemetry {
			result[key] = value
		}
		return result, nil
	}
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if value, ok := state.Telemetry[key]; ok {
			result[key] = value
		}
	}
	return result, nil
}

func (s *MemoryStore) MergeTelemetry(ctx context.Context, tenantID, entityType, entityID string, ts int64, values map[string]interface{}) error {
	_ = ctx
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	k := Key(entityType, tenantID, entityID)

	s.mu.Lock()
	old := s.states[k]
	next := ensureState(old, tenantID, entityType, entityID)
	for key, value := range values {
		existing, exists := next.Telemetry[key]
		if !exists && next.UpdatedTS > ts {
			continue
		}
		if exists && existing.TS > ts {
			continue
		}
		next.Telemetry[key] = Value{TS: ts, Value: value}
		if ts > next.UpdatedTS {
			next.UpdatedTS = ts
		}
	}
	s.states[k] = next
	change := Change{Old: cloneState(old), New: cloneState(next)}
	watches := append([]memoryWatch(nil), s.watches...)
	s.mu.Unlock()

	s.notify(k, watches, change)
	return nil
}

func (s *MemoryStore) MergeAttributes(ctx context.Context, tenantID, entityType, entityID, scope string, ts int64, values map[string]interface{}) error {
	_ = ctx
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	scope = strings.ToUpper(strings.TrimSpace(scope))
	if scope == "" {
		scope = "SERVER_SCOPE"
	}
	k := Key(entityType, tenantID, entityID)

	s.mu.Lock()
	old := s.states[k]
	next := ensureState(old, tenantID, entityType, entityID)
	if next.Attributes[scope] == nil {
		next.Attributes[scope] = map[string]Value{}
	}
	for key, value := range values {
		existing, exists := next.Attributes[scope][key]
		if !exists && next.UpdatedTS > ts {
			continue
		}
		if exists && existing.TS > ts {
			continue
		}
		next.Attributes[scope][key] = Value{TS: ts, Value: value}
		if ts > next.UpdatedTS {
			next.UpdatedTS = ts
		}
	}
	s.states[k] = next
	change := Change{Old: cloneState(old), New: cloneState(next)}
	watches := append([]memoryWatch(nil), s.watches...)
	s.mu.Unlock()

	s.notify(k, watches, change)
	return nil
}

func (s *MemoryStore) Watch(ctx context.Context, prefix string) (<-chan Change, error) {
	ch := make(chan Change, 32)
	s.mu.Lock()
	s.watches = append(s.watches, memoryWatch{prefix: prefix, ch: ch})
	s.mu.Unlock()
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		for i, watch := range s.watches {
			if watch.ch == ch {
				s.watches = append(s.watches[:i], s.watches[i+1:]...)
				break
			}
		}
		close(ch)
		s.mu.Unlock()
	}()
	return ch, nil
}

func (s *MemoryStore) notify(key string, watches []memoryWatch, change Change) {
	for _, watch := range watches {
		if watch.prefix == "" || strings.HasPrefix(key, watch.prefix) {
			select {
			case watch.ch <- change:
			default:
			}
		}
	}
}

func ensureState(state State, tenantID, entityType, entityID string) State {
	if state.Schema == "" {
		state.Schema = Schema
	}
	state.TenantID = tenantID
	state.EntityType = NormalizeEntityType(entityType)
	state.EntityID = entityID
	if state.Telemetry == nil {
		state.Telemetry = map[string]Value{}
	}
	if state.Attributes == nil {
		state.Attributes = map[string]map[string]Value{}
	}
	if state.Activity == nil {
		state.Activity = map[string]interface{}{}
	}
	return state
}

func cloneState(state State) State {
	next := state
	next.Telemetry = map[string]Value{}
	for k, v := range state.Telemetry {
		next.Telemetry[k] = v
	}
	next.Attributes = map[string]map[string]Value{}
	for scope, attrs := range state.Attributes {
		next.Attributes[scope] = map[string]Value{}
		for k, v := range attrs {
			next.Attributes[scope][k] = v
		}
	}
	next.Activity = map[string]interface{}{}
	for k, v := range state.Activity {
		next.Activity[k] = v
	}
	return next
}

func LatestAsTimeseries(values map[string]Value, strict bool) map[string][]map[string]interface{} {
	result := map[string][]map[string]interface{}{}
	for key, value := range values {
		emitted := value.Value
		if !strict {
			emitted = fmt.Sprintf("%v", value.Value)
		}
		result[key] = []map[string]interface{}{{"ts": value.TS, "value": emitted}}
	}
	return result
}

func LatestAsEntityData(values map[string]Value, keys []string) map[string]interface{} {
	result := map[string]interface{}{}
	if len(keys) == 0 {
		for key, value := range values {
			result[key] = map[string]interface{}{"ts": value.TS, "value": value.Value}
		}
		return result
	}
	for _, key := range keys {
		if value, ok := values[key]; ok {
			result[key] = map[string]interface{}{"ts": value.TS, "value": value.Value}
		}
	}
	return result
}

func ValuesToRawMap(values map[string]Value) map[string]interface{} {
	result := map[string]interface{}{}
	for key, value := range values {
		result[key] = value.Value
	}
	return result
}
