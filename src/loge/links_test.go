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
		t.Error("Wrong keys after adds: %v", 
			children.ReadKeys())
	}

	children.Set([]string{"three", "four"})

	if !compareSets(children.ReadKeys(), []string{"three", "four"}) {
		t.Error("Wrong keys after set: %v", 
			children.ReadKeys())
	}

	children.Add("five")

	if !compareSets(children.ReadKeys(), []string{"three", "four", "five"}) {
		t.Error("Wrong keys after set+add: %v", 
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

}


func TestLinkFreezing(t *testing.T) {
	var spec = map[string]string {
		"children": "obj",
		"siblings": "obj",
	}

	var links = NewObjectLinks(spec)
	var children = links.Link("children")

	if len(links.Freeze()) > 0 {
		t.Error("New links have changes")
	}

	children.Add("one")

	var changes = links.Freeze()
	if len(changes) != 1 {
		t.Errorf("Wrong number of changes (%d)", len(changes))
	}

	if changes[0].Name != "children" {
		t.Error("Wrong linkset changed")
	}

	if len(links.Freeze()) > 0 {
		t.Error("Changes on second freeze")
	}

	children.Remove("two")

	if len(links.Freeze()) > 0 {
		t.Error("Changes on noop remove")
	}

	children.Set([]string{ "three", "four" })
	links.Link("siblings").Add("nine")

	changes = links.Freeze()
	if len(changes) != 2 {
		t.Errorf("Wrong number of changes (%d)", len(changes))
	}
	
}


func compareSets(a []string, b[]string) bool {
	var sa = make([]string, len(a))
	copy(sa, a)

	var sb = make([]string, len(a))
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