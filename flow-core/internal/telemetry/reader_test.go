package telemetry

import (
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

func TestTimeseriesKeysFromQuerySupportsTBKeysParam(t *testing.T) {
	values := url.Values{}
	values.Add("keys", "temperature, humidity")
	values.Add("key", "co2")
	values.Add("key", "temperature")

	got, err := TimeseriesKeysFromQuery(values)
	if err != nil {
		t.Fatalf("TimeseriesKeysFromQuery() error = %v", err)
	}
	want := []string{"co2", "temperature", "humidity"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("TimeseriesKeysFromQuery() = %#v, want %#v", got, want)
	}
}

// Phase 5c — each requested key becomes one query against a 5-connection TSDB
// reader pool, so an unbounded keys= list starves every other reader. The cap
// must refuse the abusive request outright (not truncate it silently) while a
// fat-but-real dashboard request still passes.
func TestTimeseriesKeysFromQueryCapsFanOut(t *testing.T) {
	atLimit := url.Values{}
	for i := 0; i < MaxTimeseriesKeys; i++ {
		atLimit.Add("key", fmt.Sprintf("k%d", i))
	}
	got, err := TimeseriesKeysFromQuery(atLimit)
	if err != nil {
		t.Fatalf("%d keys rejected: %v", MaxTimeseriesKeys, err)
	}
	if len(got) != MaxTimeseriesKeys {
		t.Fatalf("got %d keys, want %d", len(got), MaxTimeseriesKeys)
	}

	over := url.Values{}
	joined := make([]string, 0, MaxTimeseriesKeys+1)
	for i := 0; i <= MaxTimeseriesKeys; i++ {
		joined = append(joined, fmt.Sprintf("k%d", i))
	}
	// Comma-joined form must be bounded too — it is the cheaper attack.
	over.Add("keys", strings.Join(joined, ","))
	if _, err := TimeseriesKeysFromQuery(over); err == nil {
		t.Fatalf("%d keys accepted; want an error", MaxTimeseriesKeys+1)
	} else if !strings.Contains(err.Error(), "max") {
		t.Fatalf("error %q does not state the limit", err)
	}
}

func TestQuestDBKVValueToTyped(t *testing.T) {
	cases := []struct {
		name   string
		kind   string
		raw    string
		strict bool
		want   interface{}
	}{
		{name: "non strict keeps string", kind: "number", raw: "21.5", strict: false, want: "21.5"},
		{name: "number", kind: "number", raw: "21.5", strict: true, want: float64(21.5)},
		{name: "boolean", kind: "boolean", raw: "true", strict: true, want: true},
		{name: "json", kind: "json", raw: `{"ok":true}`, strict: true, want: map[string]interface{}{"ok": true}},
		{name: "string", kind: "string", raw: "online", strict: true, want: "online"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := QuestDBKVValueToTyped(tc.kind, tc.raw, tc.strict)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("QuestDBKVValueToTyped() = %#v, want %#v", got, tc.want)
			}
		})
	}
}
