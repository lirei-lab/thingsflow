package twinstore

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	"flow-core/internal/natsutil"
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
	nc, err := natsutil.Connect(url, "thingsflow-flow-core")
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
	// Doc-first (milestone 4, phase 2 — dual-read window): the whole-state
	// document is the authoritative source when it carries telemetry — one
	// kv.Get, no prefix scan. ANY doc read failure (missing OR corrupt) or an
	// empty doc falls back to the per-key scan: a corrupt doc must not take
	// down reads while per-key entries are still valid (consistent with
	// GetLatestTelemetry).
	if state, err := s.GetEntityState(ctx, tenantID, entityType, entityID); err == nil {
		if len(state.Telemetry) > 0 {
			keys := make([]string, 0, len(state.Telemetry))
			for key := range state.Telemetry {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			return keys, nil
		}
	}

	// Per-key fallback (pre-flip parity): per-key entries may still be the
	// only source until the data plane is doc-only. Real infra errors (e.g.
	// the KV being unreachable) still surface here via kv.Keys().
	prefix := TelemetryPrefix(entityType, tenantID, entityID)
	kvKeys, err := s.kv.Keys(nats.Context(ctx), nats.IgnoreDeletes())
	if err == nil {
		keys := make([]string, 0)
		for _, kvKey := range kvKeys {
			if telemetryKey, ok := strings.CutPrefix(kvKey, prefix); ok && telemetryKey != "" {
				keys = append(keys, telemetryKey)
			}
		}
		sort.Strings(keys)
		return keys, nil
	}
	if !errors.Is(err, nats.ErrNoKeysFound) {
		return nil, err
	}
	return []string{}, nil
}

func (s *NATSStore) GetLatestTelemetry(ctx context.Context, tenantID, entityType, entityID string, keys []string) (map[string]Value, error) {
	// Doc-first (milestone 4, phase 2 — dual-read window): read the whole-state
	// document once and resolve the requested keys from its Telemetry map
	// (1 kv.Get instead of N per-key Gets). Keys the document does not carry
	// are filled from per-key entries; if the document is absent/unusable the
	// read degrades to the pre-flip per-key path (a corrupt doc must not take
	// down reads while the per-key entries are still valid).
	state, stateErr := s.GetEntityState(ctx, tenantID, entityType, entityID)
	result := map[string]Value{}
	if stateErr == nil {
		if len(keys) == 0 {
			for key, value := range state.Telemetry {
				result[key] = value
			}
		} else {
			for _, key := range keys {
				key = strings.TrimSpace(key)
				if key == "" {
					continue
				}
				if value, ok := state.Telemetry[key]; ok {
					result[key] = value
				}
			}
		}
	}

	if len(keys) == 0 {
		// "All telemetry": the document is authoritative when it carries any;
		// otherwise fall back to the per-key path.
		if len(result) > 0 {
			return result, nil
		}
		return s.getLatestTelemetryPerKey(ctx, tenantID, entityType, entityID, nil)
	}

	// Explicit keys: fill the ones the document did not carry from per-key.
	missing := make([]string, 0, len(keys))
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, ok := result[key]; !ok {
			missing = append(missing, key)
		}
	}
	if len(missing) == 0 {
		return result, nil
	}
	perKey, err := s.getLatestTelemetryPerKey(ctx, tenantID, entityType, entityID, missing)
	if err != nil {
		return nil, err
	}
	for key, value := range perKey {
		result[key] = value
	}
	return result, nil
}

// getLatestTelemetryPerKey is the pre-doc read path, retained as the dual-read
// fallback. keys == nil means "all telemetry keys".
func (s *NATSStore) getLatestTelemetryPerKey(ctx context.Context, tenantID, entityType, entityID string, keys []string) (map[string]Value, error) {
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
	return result, nil
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
