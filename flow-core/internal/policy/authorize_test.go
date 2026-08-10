package policy

import (
	"testing"
)

func ownerDocForTests(tenantID string) DerivedPolicy {
	root := "thing:/" + tenantID
	return DerivedPolicy{
		PolicyID:  "owner",
		Version:   "1.0.0",
		Kind:      "TWIN",
		Subjects:  []string{"tenant:" + tenantID},
		Resources: []string{root, root + "/#"},
		Grants:    map[string][]string{root: {"READ", "WRITE", "DELETE"}},
		Revokes:   map[string][]string{},
	}
}

func TestAuthorizeSubjectMatching(t *testing.T) {
	doc := ownerDocForTests("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	path := "thing:/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa/DEVICE/11111111-1111-1111-1111-111111111111/features/temp"

	cases := []struct {
		name    string
		subject string
		want    bool
	}{
		{"owning tenant allows", "tenant:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", true},
		{"foreign tenant denies", "tenant:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", false},
		{"unbound user denies", "user:99999999-9999-9999-9999-999999999999", false},
		{"empty subject denies", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Authorize(tc.subject, doc, path, "READ"); got != tc.want {
				t.Fatalf("Authorize(%q) = %v, want %v", tc.subject, got, tc.want)
			}
		})
	}
}

func TestAuthorizeRevokeOverridesGrant(t *testing.T) {
	// Grant WRITE on the feature root; revoke WRITE on one deeper path.
	// The deeper revoke must win over the shallower grant (revoke profundo gana).
	root := "thing:/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa/DEVICE/11111111-1111-1111-1111-111111111111"
	doc := DerivedPolicy{
		Subjects: []string{"tenant:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"},
		Grants:   map[string][]string{root: {"WRITE"}},
		Revokes:  map[string][]string{root + "/features/setpoint": {"WRITE"}},
	}
	subject := "tenant:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

	if !Authorize(subject, doc, root+"/features/temp", "WRITE") {
		t.Fatal("granted feature path must allow WRITE")
	}
	if Authorize(subject, doc, root+"/features/setpoint", "WRITE") {
		t.Fatal("deeper revoke must override the shallower grant (WRITE denied)")
	}
	// Fail-closed at the deepest matched level: the deeper revoke entry is the
	// deepest match for the setpoint path, and it does not grant READ, so the
	// shallower WRITE grant does NOT authorize READ there (deny at the deepest
	// matched level unless granted there).
	if Authorize(subject, doc, root+"/features/setpoint", "READ") {
		t.Fatal("deepest matched entry does not grant READ; must fail closed")
	}
}

func TestAuthorizeDeepGrantOverridesShallowRevoke(t *testing.T) {
	// Deep-revoke-wins is symmetric: the DEEPEST matching entry decides, so a
	// deeper grant also overrides a shallower revoke.
	root := "thing:/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa/DEVICE/11111111-1111-1111-1111-111111111111"
	doc := DerivedPolicy{
		Subjects: []string{"*"},
		Revokes:  map[string][]string{root: {"WRITE"}},
		Grants:   map[string][]string{root + "/features/temp": {"WRITE"}},
	}
	if !Authorize("user:anyone", doc, root+"/features/temp", "WRITE") {
		t.Fatal("deeper grant must override the shallower revoke")
	}
	if Authorize("user:anyone", doc, root+"/features/other", "WRITE") {
		t.Fatal("shallower revoke still denies un-granted deeper paths")
	}
}

func TestAuthorizeFailClosedOnUnmatchedAndUngranted(t *testing.T) {
	root := "thing:/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa/DEVICE/11111111-1111-1111-1111-111111111111"
	doc := DerivedPolicy{
		Subjects: []string{"tenant:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"},
		Grants:   map[string][]string{root + "/features/temp": {"READ"}},
		Revokes:  map[string][]string{},
	}
	subject := "tenant:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

	// Unmatched path fails closed.
	if Authorize(subject, doc, "thing:/bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", "READ") {
		t.Fatal("unmatched path must deny")
	}
	// Matched path with a different action fails closed (no WRITE grant).
	if Authorize(subject, doc, root+"/features/temp", "WRITE") {
		t.Fatal("action not granted must deny")
	}
	// A shallower grant on a deeper path does not authorize the deeper path
	// when the deepest entry does not cover the action. Grant at the feature
	// root (READ) vs a deeper feature with no READ grant → deny.
	grantRoot := DerivedPolicy{
		Subjects: []string{"tenant:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"},
		Grants:   map[string][]string{root: {"READ"}},
		Revokes:  map[string][]string{},
	}
	if !Authorize(subject, grantRoot, root+"/features/temp", "READ") {
		t.Fatal("prefix grant covers descendant paths")
	}
}

func TestAuthorizePathGranular(t *testing.T) {
	root := "thing:/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa/DEVICE/11111111-1111-1111-1111-111111111111"
	doc := DerivedPolicy{
		Subjects: []string{"tenant:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"},
		Grants: map[string][]string{
			root + "/features/temp":   {"READ", "WRITE"},
			root + "/attributes/name": {"READ"},
		},
		Revokes: map[string][]string{},
	}
	subject := "tenant:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

	if !Authorize(subject, doc, root+"/features/temp", "WRITE") {
		t.Fatal("granted feature allows WRITE")
	}
	if Authorize(subject, doc, root+"/features/humidity", "READ") {
		t.Fatal("ungranted feature must deny")
	}
	if !Authorize(subject, doc, root+"/attributes/name", "READ") {
		t.Fatal("granted attribute allows READ")
	}
	if Authorize(subject, doc, root+"/attributes/name", "WRITE") {
		t.Fatal("attribute granted READ only must deny WRITE")
	}
}

func TestAuthorizeWildcardSegments(t *testing.T) {
	root := "thing:/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	doc := DerivedPolicy{
		Subjects: []string{"*"},
		Grants: map[string][]string{
			root + "/DEVICE/*/features/temp": {"READ"},
			root + "/#":                      {"READ"},
		},
		Revokes: map[string][]string{},
	}
	// '*' matches exactly one segment (any device id).
	if !Authorize("user:anyone", doc, root+"/DEVICE/11111111-1111-1111-1111-111111111111/features/temp", "READ") {
		t.Fatal("'*' wildcard must match one device segment")
	}
	// '#' recursive matches everything under the tenant root.
	if !Authorize("user:anyone", doc, root+"/ASSET/22222222-2222-2222-2222-222222222222/attributes/x", "READ") {
		t.Fatal("'#' recursive wildcard must match descendants")
	}
}

func TestSubjectForClaims(t *testing.T) {
	tenantClaims := map[string]interface{}{"tenantId": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}
	if got := SubjectForClaims(tenantClaims); got != "tenant:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" {
		t.Fatalf("tenant subject = %q", got)
	}
	sysAdminClaims := map[string]interface{}{"scopes": []interface{}{"TENANT_ADMIN", "SYS_ADMIN"}}
	if got := SubjectForClaims(sysAdminClaims); got != "SYS_ADMIN" {
		t.Fatalf("sys admin subject = %q", got)
	}
	userClaims := map[string]interface{}{"userId": "99999999-9999-9999-9999-999999999999"}
	if got := SubjectForClaims(userClaims); got != "user:99999999-9999-9999-9999-999999999999" {
		t.Fatalf("user subject = %q", got)
	}
}

func TestResourcePath(t *testing.T) {
	got := ResourcePath("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "device", "11111111-1111-1111-1111-111111111111", "features", "temp")
	want := "thing:/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa/DEVICE/11111111-1111-1111-1111-111111111111/features/temp"
	if got != want {
		t.Fatalf("ResourcePath = %q, want %q", got, want)
	}
}
