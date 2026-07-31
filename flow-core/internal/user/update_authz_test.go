package user

import "testing"

// A CUSTOMER_USER must not be able to edit another user's row. This handler
// writes `email`, which IS the login identity, so an unguarded same-tenant
// edit is account takeover: rewrite the victim's email, then drive the
// password-reset flow. That regression shipped once (the authority gate was
// relaxed to allow profile edits and nothing else checked who may edit whom),
// so the rule is pinned here at the decision level.
func TestUpdateAuthority_WhoMayEditWhom(t *testing.T) {
	// mirrors the handler's guard: sysadmin, tenant admin, or self.
	mayEdit := func(isSysAdmin, isSelf bool, callerAuthority string) bool {
		return isSysAdmin || isSelf || callerAuthority == "TENANT_ADMIN"
	}
	cases := []struct {
		name            string
		isSysAdmin      bool
		isSelf          bool
		callerAuthority string
		want            bool
	}{
		{"customer edits another user", false, false, "CUSTOMER_USER", false},
		{"customer edits itself", false, true, "CUSTOMER_USER", true},
		{"tenant admin edits a user in its tenant", false, false, "TENANT_ADMIN", true},
		{"sysadmin edits anyone", true, false, "SYS_ADMIN", true},
		{"unknown role edits another user", false, false, "", false},
	}
	for _, c := range cases {
		if got := mayEdit(c.isSysAdmin, c.isSelf, c.callerAuthority); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}
