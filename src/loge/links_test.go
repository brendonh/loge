package loge

import (
	"testing"
	"sort"
)

func TestLinks(t *testing.T) {
	var spec = map[string]string {
		"children": "obj",
	}

	var links = NewObjectLinks(spec)

	var children = links.Link("children")

	children.Add("one")
	children.Add("two")

	if !compareSets(children.ReadKeys(), []string{"one", "two"}) {
		t.Errorf("Wrong keys after adds: %v",
			children.ReadKeys())
	}

	children.Set([]string{"three", "four"})

	if !compareSets(children.ReadKeys(), []string{"three", "four"}) {
		t.Errorf("Wrong keys after set: %v",
			children.ReadKeys())
	}

	children.Add("five")

	if !compareSets(children.ReadKeys(), []string{"three", "four", "five"}) {
		t.Errorf("Wrong keys after set+add: %v",
			children.ReadKeys())
	}

	if !children.Has("four") {
		t.Error("Key four missing")
	}

	children.Remove("four")

	if children.Has("four") {
		t.Error("Key four present after removal")
	}

	if children.Has("one") {
		t.Error("Key one present")
	}

	children.Add("four")

	if !children.Has("four") {
		t.Error("Key four missing")
	}

}

func compareSets(a []string, b[]string) bool {
	var sa = make([]string, len(a))
	copy(sa, a)

	var sb = make([]string, len(b))
	copy(sb, b)

	sort.Strings(sa)
	sort.Strings(sb)

	if len(sa) != len(sb) {
		return false
	}

	for i, _ := range sa {
		if sa[i] != sb[i] {
			return false
		}
	}

	return true
}

func dumpLinkSet(t *testing.T, ls *LinkSet) {
	t.Log("------- Original ---------\n")
	for k, v := range ls.Original {
		t.Logf("%s => %v\n", k, v)
	}

	t.Log("------- Added ---------\n")
	for k, v := range ls.Added {
		t.Logf("%s => %v\n", k, v)
	}

	t.Log("------- Removed ---------\n")
	for k, v := range ls.Removed {
		t.Logf("%s => %v\n", k, v)
	}
}