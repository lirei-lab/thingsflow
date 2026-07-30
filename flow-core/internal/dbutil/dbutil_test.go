package dbutil

import (
	"reflect"
	"testing"
)

func TestNullStr(t *testing.T) {
	if NullStr("").Valid {
		t.Error("empty should be invalid")
	}
	n := NullStr("x")
	if !n.Valid || n.String != "x" {
		t.Errorf("got %+v, want {x true}", n)
	}
}

func TestNullUUID(t *testing.T) {
	cases := []struct {
		in    string
		valid bool
	}{
		{"", false},
		{"13814000-1dd2-11b2-8080-808080808080", false}, // TB nil sentinel
		{"abc", true},
	}
	for _, c := range cases {
		if got := NullUUID(c.in).Valid; got != c.valid {
			t.Errorf("NullUUID(%q).Valid = %v, want %v", c.in, got, c.valid)
		}
	}
}

func TestJSONOrNil(t *testing.T) {
	if got := JSONOrNil(nil); got != nil {
		t.Errorf("nil → %v, want nil", got)
	}
	if got := JSONOrNil(map[string]int{"a": 1}); got != `{"a":1}` {
		t.Errorf("map → %v, want JSON string", got)
	}
}

func TestParsePgTextArray(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"{}", []string{}},
		{"{}", []string{}},
		{"", []string{}},
		{"{a,b,c}", []string{"a", "b", "c"}},
		{`{"a b","c,d",e}`, []string{"a b", "c,d", "e"}},
		{"{single}", []string{"single"}},
	}
	for _, c := range cases {
		got := ParsePgTextArray([]byte(c.in))
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("ParsePgTextArray(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}
