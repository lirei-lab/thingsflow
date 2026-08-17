package twinstore

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

type fakeKVEntry struct {
	bucket string
	key    string
	value  []byte
	// op parametrizes Operation(); the zero value is nats.KeyValuePut, so
	// existing fixtures keep their old behaviour while watch tests can
	// exercise delete/purge entries.
	op nats.KeyValueOp
}

func (e fakeKVEntry) Bucket() string             { return e.bucket }
func (e fakeKVEntry) Key() string                { return e.key }
func (e fakeKVEntry) Value() []byte              { return e.value }
func (e fakeKVEntry) Revision() uint64           { return 1 }
func (e fakeKVEntry) Created() time.Time         { return time.UnixMilli(0) }
func (e fakeKVEntry) Delta() uint64              { return 0 }
func (e fakeKVEntry) Operation() nats.KeyValueOp { return e.op }

// fakeKeyWatcher is a controllable nats.KeyWatcher: tests push entries into
// updates (buffered) and close it when done; Stop is sync.Once-guarded because
// NATSStore.Watch defers it after the updates channel already closed.
type fakeKeyWatcher struct {
	updates  chan nats.KeyValueEntry
	stopOnce sync.Once
}

func newFakeKeyWatcher() *fakeKeyWatcher {
	return &fakeKeyWatcher{updates: make(chan nats.KeyValueEntry, 16)}
}

func (w *fakeKeyWatcher) Context() context.Context           { return context.Background() }
func (w *fakeKeyWatcher) Updates() <-chan nats.KeyValueEntry { return w.updates }
func (w *fakeKeyWatcher) Stop() error                        { w.stopOnce.Do(func() {}); return nil }
func (w *fakeKeyWatcher) push(e nats.KeyValueEntry)          { w.updates <- e }
func (w *fakeKeyWatcher) close()                             { close(w.updates) }

type fakeNATSKeyValue struct {
	values map[string][]byte
	// watcher, when set, is returned by Watch so tests can drive the
	// NATSStore.Watch decode loop.
	watcher *fakeKeyWatcher
}

func (kv fakeNATSKeyValue) Get(key string) (nats.KeyValueEntry, error) {
	value, ok := kv.values[key]
	if !ok {
		return nil, nats.ErrKeyNotFound
	}
	return fakeKVEntry{bucket: "twin_state", key: key, value: value}, nil
}

func (kv fakeNATSKeyValue) Keys(opts ...nats.WatchOpt) ([]string, error) {
	keys := make([]string, 0, len(kv.values))
	for key := range kv.values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

func (kv fakeNATSKeyValue) GetRevision(string, uint64) (nats.KeyValueEntry, error) {
	return nil, errors.New("not implemented")
}
func (kv fakeNATSKeyValue) Put(string, []byte) (uint64, error) {
	return 0, errors.New("not implemented")
}
func (kv fakeNATSKeyValue) PutString(string, string) (uint64, error) {
	return 0, errors.New("not implemented")
}
func (kv fakeNATSKeyValue) Create(string, []byte) (uint64, error) {
	return 0, errors.New("not implemented")
}
func (kv fakeNATSKeyValue) Update(string, []byte, uint64) (uint64, error) {
	return 0, errors.New("not implemented")
}
func (kv fakeNATSKeyValue) Delete(string, ...nats.DeleteOpt) error {
	return errors.New("not implemented")
}
func (kv fakeNATSKeyValue) Purge(string, ...nats.DeleteOpt) error {
	return errors.New("not implemented")
}
func (kv fakeNATSKeyValue) Watch(string, ...nats.WatchOpt) (nats.KeyWatcher, error) {
	if kv.watcher != nil {
		return kv.watcher, nil
	}
	return nil, errors.New("not implemented")
}
func (kv fakeNATSKeyValue) WatchAll(...nats.WatchOpt) (nats.KeyWatcher, error) {
	return nil, errors.New("not implemented")
}
func (kv fakeNATSKeyValue) ListKeys(...nats.WatchOpt) (nats.KeyLister, error) {
	return nil, errors.New("not implemented")
}
func (kv fakeNATSKeyValue) History(string, ...nats.WatchOpt) ([]nats.KeyValueEntry, error) {
	return nil, errors.New("not implemented")
}
func (kv fakeNATSKeyValue) Bucket() string { return "twin_state" }
func (kv fakeNATSKeyValue) PurgeDeletes(...nats.PurgeOpt) error {
	return errors.New("not implemented")
}
func (kv fakeNATSKeyValue) Status() (nats.KeyValueStatus, error) {
	return nil, errors.New("not implemented")
}

func TestNATSStoreReadsPerTelemetryKeyValues(t *testing.T) {
	store := NewNATSStore(fakeNATSKeyValue{values: map[string][]byte{
		"DEVICE.tenant-1.device-1.telemetry.temperature": []byte(`{"ts":1000,"value":21.5}`),
		"DEVICE.tenant-1.device-1.telemetry.active":      []byte(`{"ts":1100,"value":true}`),
		"DEVICE.tenant-1.device-2.telemetry.temperature": []byte(`{"ts":1200,"value":99}`),
	}})

	keys, err := store.GetTelemetryKeys(context.Background(), "tenant-1", "DEVICE", "device-1")
	if err != nil {
		t.Fatalf("keys: %v", err)
	}
	if len(keys) != 2 || keys[0] != "active" || keys[1] != "temperature" {
		t.Fatalf("keys = %v, want [active temperature]", keys)
	}

	latest, err := store.GetLatestTelemetry(context.Background(), "tenant-1", "DEVICE", "device-1", []string{"temperature"})
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if len(latest) != 1 {
		t.Fatalf("latest length = %d, want 1", len(latest))
	}
	if got := latest["temperature"].Value; got != 21.5 {
		t.Fatalf("temperature value = %v, want 21.5", got)
	}
	if got := latest["temperature"].TS; got != int64(1000) {
		t.Fatalf("temperature ts = %d, want 1000", got)
	}
}

func TestNATSStoreGetLatestTelemetryDocFirst(t *testing.T) {
	store := NewNATSStore(fakeNATSKeyValue{values: map[string][]byte{
		"DEVICE.tenant-1.device-1": []byte(
			`{"schema":"thingsflow.twin-state.v1","tenantId":"tenant-1","entityType":"DEVICE","entityId":"device-1",` +
				`"updatedTs":1500,"telemetry":{"temperature":{"ts":1500,"value":22.5},"humidity":{"ts":1500,"value":60}},` +
				`"attributes":{},"activity":{}}`),
	}})

	// Explicit keys resolved from the doc (1 kv.Get; there are no per-key
	// entries in this fixture, so any per-key read would return nothing).
	latest, err := store.GetLatestTelemetry(context.Background(), "tenant-1", "DEVICE", "device-1", []string{"temperature", "humidity"})
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if len(latest) != 2 {
		t.Fatalf("latest length = %d, want 2 (doc-first)", len(latest))
	}
	if v := latest["temperature"]; v.TS != 1500 || v.Value != 22.5 {
		t.Fatalf("temperature = %#v, want ts=1500 value=22.5", v)
	}

	// All-keys read is doc-first too.
	all, err := store.GetLatestTelemetry(context.Background(), "tenant-1", "DEVICE", "device-1", nil)
	if err != nil {
		t.Fatalf("all: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("all length = %d, want 2", len(all))
	}
}

func TestNATSStoreGetLatestTelemetryDocAbsentFallsBackToPerKey(t *testing.T) {
	store := NewNATSStore(fakeNATSKeyValue{values: map[string][]byte{
		"DEVICE.tenant-1.device-1.telemetry.temperature": []byte(`{"ts":1000,"value":21.5}`),
	}})

	latest, err := store.GetLatestTelemetry(context.Background(), "tenant-1", "DEVICE", "device-1", []string{"temperature"})
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if v := latest["temperature"]; v.Value != 21.5 || v.TS != 1000 {
		t.Fatalf("temperature = %#v, want per-key ts=1000 value=21.5", v)
	}

	all, err := store.GetLatestTelemetry(context.Background(), "tenant-1", "DEVICE", "device-1", nil)
	if err != nil {
		t.Fatalf("all: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("all length = %d, want 1 (per-key fallback)", len(all))
	}
}

func TestNATSStoreGetLatestTelemetryDocFillsMissingPerKeyKeys(t *testing.T) {
	// Transition safety: the doc carries "a", per-key carries "b" — both must
	// come back (union) so a mixed device does not lose the data-plane key.
	store := NewNATSStore(fakeNATSKeyValue{values: map[string][]byte{
		"DEVICE.tenant-1.device-1": []byte(
			`{"schema":"thingsflow.twin-state.v1","tenantId":"tenant-1","entityType":"DEVICE","entityId":"device-1",` +
				`"updatedTs":1500,"telemetry":{"a":{"ts":1500,"value":1}},"attributes":{},"activity":{}}`),
		"DEVICE.tenant-1.device-1.telemetry.b": []byte(`{"ts":1600,"value":2}`),
	}})

	latest, err := store.GetLatestTelemetry(context.Background(), "tenant-1", "DEVICE", "device-1", []string{"a", "b"})
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if len(latest) != 2 {
		t.Fatalf("latest length = %d, want 2 (doc a + per-key b)", len(latest))
	}
	// JSON numbers decode to float64; Value.Value is interface{}.
	if v := latest["a"]; v.Value != float64(1) {
		t.Fatalf("a = %#v, want doc value 1", v)
	}
	if v := latest["b"]; v.Value != float64(2) {
		t.Fatalf("b = %#v, want per-key value 2", v)
	}
}

func TestNATSStoreGetTelemetryKeysDocFirst(t *testing.T) {
	store := NewNATSStore(fakeNATSKeyValue{values: map[string][]byte{
		"DEVICE.tenant-1.device-1": []byte(
			`{"schema":"thingsflow.twin-state.v1","tenantId":"tenant-1","entityType":"DEVICE","entityId":"device-1",` +
				`"updatedTs":1500,"telemetry":{"z":{"ts":1500,"value":1},"a":{"ts":1500,"value":2}},"attributes":{},"activity":{}}`),
		// A per-key-only entry that must NOT surface while the doc is
		// authoritative (the doc wins; per-key is the fallback only).
		"DEVICE.tenant-1.device-1.telemetry.perkey": []byte(`{"ts":1600,"value":3}`),
	}})

	keys, err := store.GetTelemetryKeys(context.Background(), "tenant-1", "DEVICE", "device-1")
	if err != nil {
		t.Fatalf("keys: %v", err)
	}
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "z" {
		t.Fatalf("keys = %v, want [a z] (doc-first, sorted; per-key entry excluded)", keys)
	}
}

func TestNATSStoreGetTelemetryKeysDocEmptyFallsBackToPerKey(t *testing.T) {
	store := NewNATSStore(fakeNATSKeyValue{values: map[string][]byte{
		"DEVICE.tenant-1.device-1": []byte(
			`{"schema":"thingsflow.twin-state.v1","tenantId":"tenant-1","entityType":"DEVICE","entityId":"device-1",` +
				`"updatedTs":0,"telemetry":{},"attributes":{},"activity":{}}`),
		"DEVICE.tenant-1.device-1.telemetry.temperature": []byte(`{"ts":1000,"value":21.5}`),
	}})

	keys, err := store.GetTelemetryKeys(context.Background(), "tenant-1", "DEVICE", "device-1")
	if err != nil {
		t.Fatalf("keys: %v", err)
	}
	if len(keys) != 1 || keys[0] != "temperature" {
		t.Fatalf("keys = %v, want [temperature] (per-key fallback for empty doc)", keys)
	}
}

// readChange receives one Change with a deadline so a decode regression fails
// fast instead of hanging the suite.
func readChange(t *testing.T, changes <-chan Change) Change {
	t.Helper()
	select {
	case change, ok := <-changes:
		if !ok {
			t.Fatal("watch channel closed before delivering a change")
		}
		return change
	case <-time.After(2 * time.Second):
		t.Fatal("no change arrived within 2s")
	}
	return Change{}
}

func TestNATSStoreWatchDecodesFullStateDocument(t *testing.T) {
	watcher := newFakeKeyWatcher()
	store := NewNATSStore(fakeNATSKeyValue{watcher: watcher})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	changes, err := store.Watch(ctx, "")
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	watcher.push(fakeKVEntry{bucket: "twin_state", key: "DEVICE.tenant-1.device-1", value: []byte(
		`{"schema":"thingsflow.twin-state.v1","tenantId":"tenant-1","entityType":"DEVICE","entityId":"device-1",` +
			`"updatedTs":1500,"telemetry":{"temperature":{"ts":1500,"value":22.5}},` +
			`"attributes":{"SERVER_SCOPE":{"site":{"ts":1400,"value":"hq"}}}}`)})

	change := readChange(t, changes)
	if change.New.EntityID != "device-1" || change.New.TenantID != "tenant-1" || change.New.EntityType != "DEVICE" {
		t.Fatalf("decoded identity = %#v", change.New)
	}
	if v := change.New.Telemetry["temperature"]; v.TS != 1500 || v.Value != 22.5 {
		t.Fatalf("telemetry = %#v, want temperature ts=1500 value=22.5", change.New.Telemetry)
	}
	if v := change.New.Attributes["SERVER_SCOPE"]["site"]; v.Value != "hq" {
		t.Fatalf("attributes = %#v, want SERVER_SCOPE site=hq", change.New.Attributes)
	}
	// The NATS path never carries Old — the watch-loop consumer reconstructs
	// it from its last-known map. Pin that contract here.
	if change.Old.EntityID != "" || len(change.Old.Telemetry) != 0 {
		t.Fatalf("Change.Old must be zero on the NATS path, got %#v", change.Old)
	}
}

func TestNATSStoreWatchSynthesizesPerKeyState(t *testing.T) {
	watcher := newFakeKeyWatcher()
	store := NewNATSStore(fakeNATSKeyValue{watcher: watcher})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	changes, err := store.Watch(ctx, "")
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	// Bare per-key Value, the shape the Bento latest-kv pipeline writes.
	watcher.push(fakeKVEntry{bucket: "twin_state",
		key: "DEVICE.tenant-1.device-1.telemetry.power", value: []byte(`{"ts":2000,"value":5.5}`)})

	change := readChange(t, changes)
	if change.New.EntityID != "device-1" || change.New.TenantID != "tenant-1" {
		t.Fatalf("synthesized identity = %#v", change.New)
	}
	if len(change.New.Telemetry) != 1 {
		t.Fatalf("synthesized state must carry exactly the written key, got %#v", change.New.Telemetry)
	}
	if v := change.New.Telemetry["power"]; v.TS != 2000 || v.Value != 5.5 {
		t.Fatalf("power = %#v, want ts=2000 value=5.5", v)
	}
	if len(change.New.Attributes) != 0 {
		t.Fatalf("synthesized state must have EMPTY attributes (the watch consumer merges, not replaces), got %#v", change.New.Attributes)
	}
	if change.Old.EntityID != "" {
		t.Fatalf("Change.Old must be zero on the NATS path, got %#v", change.Old)
	}
}

func TestNATSStoreWatchSkipsUndecodableEntries(t *testing.T) {
	watcher := newFakeKeyWatcher()
	store := NewNATSStore(fakeNATSKeyValue{watcher: watcher})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	changes, err := store.Watch(ctx, "")
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	// A KV delete tombstone (empty value) and garbage must both be skipped;
	// only the valid entry after them may come out.
	watcher.push(fakeKVEntry{bucket: "twin_state", key: "DEVICE.tenant-1.device-1",
		value: nil, op: nats.KeyValueDelete})
	watcher.push(fakeKVEntry{bucket: "twin_state", key: "not-a-twin-key", value: []byte(`{"x":1}`)})
	watcher.push(fakeKVEntry{bucket: "twin_state",
		key: "DEVICE.tenant-1.device-1.telemetry.co2", value: []byte(`{"ts":3000,"value":400}`)})
	watcher.close()

	change := readChange(t, changes)
	if _, ok := change.New.Telemetry["co2"]; !ok {
		t.Fatalf("first delivered change = %#v, want the co2 per-key state (tombstone/garbage skipped)", change.New)
	}
	if _, ok := <-changes; ok {
		t.Fatal("extra change delivered for undecodable entries")
	}
}
