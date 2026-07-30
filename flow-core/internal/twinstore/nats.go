package twinstore

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

type NATSStore struct {
	kv nats.KeyValue
}

func NewNATSStore(kv nats.KeyValue) *NATSStore {
	return &NATSStore{kv: kv}
}

func ConnectNATS(url, bucket string) (*nats.Conn, *NATSStore, error) {
	if url == "" {
		url = nats.DefaultURL
	}
	if bucket == "" {
		bucket = "twin_state"
	}
	nc, err := nats.Connect(url, nats.Timeout(5*time.Second), nats.Name("thingsflow-flow-core"))
	if err != nil {
		return nil, nil, err
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, nil, err
	}
	kv, err := js.KeyValue(bucket)
	if err != nil {
		nc.Close()
		return nil, nil, err
	}
	return nc, NewNATSStore(kv), nil
}

func (s *NATSStore) GetEntityState(ctx context.Context, tenantID, entityType, entityID string) (State, error) {
	_ = ctx
	entry, err := s.kv.Get(Key(entityType, tenantID, entityID))
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			return State{}, ErrNotFound
		}
		return State{}, err
	}
	return decodeState(entry.Value())
}

func (s *NATSStore) GetTelemetryKeys(ctx context.Context, tenantID, entityType, entityID string) ([]string, error) {
	prefix := TelemetryPrefix(entityType, tenantID, entityID)
	kvKeys, err := s.kv.Keys(nats.Context(ctx), nats.IgnoreDeletes())
	if err == nil {
		keys := make([]string, 0)
		for _, kvKey := range kvKeys {
			if telemetryKey, ok := strings.CutPrefix(kvKey, prefix); ok && telemetryKey != "" {
				keys = append(keys, telemetryKey)
			}
		}
		if len(keys) > 0 {
			sort.Strings(keys)
			return keys, nil
		}
	} else if !errors.Is(err, nats.ErrNoKeysFound) {
		return nil, err
	}

	mem := NewMemoryStore()
	state, err := s.GetEntityState(ctx, tenantID, entityType, entityID)
	if err != nil {
		return nil, err
	}
	mem.states[Key(entityType, tenantID, entityID)] = state
	return mem.GetTelemetryKeys(ctx, tenantID, entityType, entityID)
}

func (s *NATSStore) GetLatestTelemetry(ctx context.Context, tenantID, entityType, entityID string, keys []string) (map[string]Value, error) {
	if len(keys) == 0 {
		var err error
		keys, err = s.GetTelemetryKeys(ctx, tenantID, entityType, entityID)
		if err != nil {
			return nil, err
		}
	}
	result := map[string]Value{}
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		entry, err := s.kv.Get(TelemetryKey(entityType, tenantID, entityID, key))
		if err != nil {
			if errors.Is(err, nats.ErrKeyNotFound) || errors.Is(err, nats.ErrKeyDeleted) {
				continue
			}
			return nil, err
		}
		value, err := decodeValue(entry.Value())
		if err != nil {
			return nil, err
		}
		result[key] = value
	}
	if len(result) > 0 {
		return result, nil
	}

	mem := NewMemoryStore()
	state, err := s.GetEntityState(ctx, tenantID, entityType, entityID)
	if err != nil {
		return nil, err
	}
	mem.states[Key(entityType, tenantID, entityID)] = state
	return mem.GetLatestTelemetry(ctx, tenantID, entityType, entityID, keys)
}

func (s *NATSStore) MergeTelemetry(ctx context.Context, tenantID, entityType, entityID string, ts int64, values map[string]interface{}) error {
	return s.merge(ctx, tenantID, entityType, entityID, func(state State) State {
		mem := NewMemoryStore()
		mem.states[Key(entityType, tenantID, entityID)] = state
		_ = mem.MergeTelemetry(ctx, tenantID, entityType, entityID, ts, values)
		next, _ := mem.GetEntityState(ctx, tenantID, entityType, entityID)
		return next
	})
}

func (s *NATSStore) MergeAttributes(ctx context.Context, tenantID, entityType, entityID, scope string, ts int64, values map[string]interface{}) error {
	return s.merge(ctx, tenantID, entityType, entityID, func(state State) State {
		mem := NewMemoryStore()
		mem.states[Key(entityType, tenantID, entityID)] = state
		_ = mem.MergeAttributes(ctx, tenantID, entityType, entityID, scope, ts, values)
		next, _ := mem.GetEntityState(ctx, tenantID, entityType, entityID)
		return next
	})
}

func (s *NATSStore) Watch(ctx context.Context, prefix string) (<-chan Change, error) {
	if prefix == "" {
		prefix = ">"
	} else if !strings.HasSuffix(prefix, ">") {
		prefix += ">"
	}
	watcher, err := s.kv.Watch(prefix)
	if err != nil {
		return nil, err
	}
	out := make(chan Change, 32)
	go func() {
		defer close(out)
		defer watcher.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case entry, ok := <-watcher.Updates():
				if !ok {
					return
				}
				if entry == nil {
					continue
				}
				state, err := decodeState(entry.Value())
				if err != nil {
					entityType, tenantID, entityID, telemetryKey, ok := ParseTelemetryKey(entry.Key())
					if !ok {
						continue
					}
					value, err := decodeValue(entry.Value())
					if err != nil {
						continue
					}
					state = ensureState(State{}, tenantID, entityType, entityID)
					state.UpdatedTS = value.TS
					state.Telemetry[telemetryKey] = value
				}
				select {
				case out <- Change{New: state}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

func (s *NATSStore) merge(ctx context.Context, tenantID, entityType, entityID string, apply func(State) State) error {
	key := Key(entityType, tenantID, entityID)
	for attempt := 0; attempt < 8; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		entry, err := s.kv.Get(key)
		if err != nil {
			if errors.Is(err, nats.ErrKeyNotFound) {
				next := apply(State{})
				payload, err := json.Marshal(next)
				if err != nil {
					return err
				}
				if _, err := s.kv.Create(key, payload); err == nil {
					return nil
				}
				time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
				continue
			}
			return err
		}
		current, err := decodeState(entry.Value())
		if err != nil {
			return err
		}
		next := apply(current)
		payload, err := json.Marshal(next)
		if err != nil {
			return err
		}
		if _, err := s.kv.Update(key, payload, entry.Revision()); err == nil {
			return nil
		}
		time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
	}
	return errors.New("nats kv update conflict")
}

// errNotState reports a payload that parsed as JSON but is not a State document.
var errNotState = errors.New("payload is not a twin state document")

// decodeState parses a full State document.
//
// The strictness matters: two different shapes live in this bucket. flow-core
// writes whole State documents, while the Bento latest-kv pipeline writes one
// bare Value per telemetry key ({"ts":…,"value":…}) under a
// DEVICE.<tenant>.<entity>.telemetry.<key> key. Go's json.Unmarshal ignores
// unknown fields, so a bare Value decodes into an *empty* State without error —
// and callers that branch on that error then treat every data-plane update as a
// state with no telemetry.
//
// That is precisely what silently disabled live WebSocket updates: the watcher
// took the non-error path, produced an empty State, and the change diff found
// nothing to broadcast. Dashboards showed the value present at subscribe time
// and never moved, while REST-written telemetry (a real State document) pushed
// normally — which made it look like the WebSocket layer was fine.
//
// Requiring an identifying field forces the caller's per-key fallback to run for
// data-plane writes, which is what actually understands them.
func decodeState(payload []byte) (State, error) {
	var state State
	if err := json.Unmarshal(payload, &state); err != nil {
		return State{}, err
	}
	if state.EntityID == "" && len(state.Telemetry) == 0 &&
		len(state.Attributes) == 0 && len(state.Activity) == 0 {
		return State{}, errNotState
	}
	state = ensureState(state, state.TenantID, state.EntityType, state.EntityID)
	return state, nil
}

func decodeValue(payload []byte) (Value, error) {
	var value Value
	if err := json.Unmarshal(payload, &value); err != nil {
		return Value{}, err
	}
	return value, nil
}
