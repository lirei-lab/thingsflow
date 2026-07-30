package twinstore

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

type fakeKVEntry struct {
	bucket string
	key    string
	value  []byte
}

func (e fakeKVEntry) Bucket() string             { return e.bucket }
func (e fakeKVEntry) Key() string                { return e.key }
func (e fakeKVEntry) Value() []byte              { return e.value }
func (e fakeKVEntry) Revision() uint64           { return 1 }
func (e fakeKVEntry) Created() time.Time         { return time.UnixMilli(0) }
func (e fakeKVEntry) Delta() uint64              { return 0 }
func (e fakeKVEntry) Operation() nats.KeyValueOp { return nats.KeyValuePut }

type fakeNATSKeyValue struct {
	values map[string][]byte
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
