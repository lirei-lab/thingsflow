package telemetry

import (
	"net/url"
	"reflect"
	"testing"
)

func TestTimeseriesKeysFromQuerySupportsTBKeysParam(t *testing.T) {
	values := url.Values{}
	values.Add("keys", "temperature, humidity")
	values.Add("key", "co2")
	values.Add("key", "temperature")

	got := TimeseriesKeysFromQuery(values)
	want := []string{"co2", "temperature", "humidity"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("TimeseriesKeysFromQuery() = %#v, want %#v", got, want)
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
