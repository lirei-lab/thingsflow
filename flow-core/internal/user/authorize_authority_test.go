package user

import "testing"

// authorizeAuthority is the privilege gate for setting/changing a user's
// authority. These cases pin both the security rules (no escalation) and the
// regression that a profile edit which does NOT touch authority must be
// allowed for any role.
func TestAuthorizeAuthority(t *testing.T) {
	const TA = "aaaa"
	cases := []struct {
		name                      string
		callerAuth, callerTenant  string
		isSysAdmin                bool
		targetAuth, targetTenant  string
		isSelf, changingAuthority bool
		want                      bool
	}{
		// Profile edits that do NOT change authority — allowed for everyone.
		{"customer self profile edit", "CUSTOMER_USER", TA, false, "CUSTOMER_USER", TA, true, false, true},
		{"tenant self profile edit", "TENANT_ADMIN", TA, false, "TENANT_ADMIN", TA, true, false, true},
		{"tenant edits a user, authority unchanged", "TENANT_ADMIN", TA, false, "CUSTOMER_USER", TA, false, false, true},
		// Self-elevation — blocked unless already SYS_ADMIN.
		{"tenant self-elevate to SYS_ADMIN", "TENANT_ADMIN", TA, false, "SYS_ADMIN", TA, true, true, false},
		{"customer self-elevate to TENANT_ADMIN", "CUSTOMER_USER", TA, false, "TENANT_ADMIN", TA, true, true, false},
		{"sysadmin self stays sysadmin (no change)", "SYS_ADMIN", "", true, "SYS_ADMIN", "", true, false, true},
		// Creating/elevating others.
		{"tenant creates SYS_ADMIN", "TENANT_ADMIN", TA, false, "SYS_ADMIN", TA, false, true, false},
		{"customer creates TENANT_ADMIN", "CUSTOMER_USER", TA, false, "TENANT_ADMIN", TA, false, true, false},
		{"tenant creates TENANT_ADMIN in own tenant", "TENANT_ADMIN", TA, false, "TENANT_ADMIN", TA, false, true, true},
		{"tenant creates TENANT_ADMIN in other tenant", "TENANT_ADMIN", TA, false, "TENANT_ADMIN", "bbbb", false, true, false},
		{"sysadmin creates SYS_ADMIN", "SYS_ADMIN", "", true, "SYS_ADMIN", TA, false, true, true},
		{"unknown authority by non-sysadmin", "TENANT_ADMIN", TA, false, "wizard", TA, false, true, false},
	}
	for _, c := range cases {
		got := authorizeAuthority(c.callerAuth, c.callerTenant, c.isSysAdmin, c.targetAuth, c.targetTenant, c.isSelf, c.changingAuthority)
		if got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}
