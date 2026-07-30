package main

import "strings"

// isDeclaredNoGoal reports whether method+path is one of the UI endpoints the
// contract deliberately declares out of scope (declaredNoGoals, generated from
// the contract JSON).
//
// The distinction matters because the two cases need opposite failure modes.
// A declared no-goal keeps the forgiving response — empty PageData on GET — so
// the 89 screens the team chose not to implement still render. An *undeclared*
// path answered the same way is how 34 endpoints once went silently
// unimplemented: the UI showed an empty list and nobody saw a failure. Those
// now get a loud 404 for every method, GET included.
//
// "{id}" in a pattern matches exactly one non-empty path segment; everything
// else matches literally.
func isDeclaredNoGoal(method, path string) bool {
	segs := strings.Split(strings.TrimSuffix(path, "/"), "/")
	for _, ng := range declaredNoGoals {
		if ng.method != method {
			continue
		}
		pat := strings.Split(ng.pattern, "/")
		if len(pat) != len(segs) {
			continue
		}
		ok := true
		for i := range pat {
			if pat[i] == "{id}" {
				if segs[i] == "" {
					ok = false
					break
				}
				continue
			}
			if pat[i] != segs[i] {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}
