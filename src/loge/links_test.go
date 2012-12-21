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
	children := links.Link("children")

	links.Freeze()

	if len(children.Previous) > 0 {
		t.Error("New links have changes")
		dumpLinkSet(t, children)
	}

	children.Set([]string { "one", "two", "three" })
	children.Remove("two")

	links.Freeze()

	if !compareSets(children.ReadKeys(), []string{ "one", "three" }) {
		t.Errorf("Wrong keys after freeze: %v", children.ReadKeys())
		dumpLinkSet(t, children)
	}

	if len(children.Current) != 2 {
		t.Errorf("Wrong current keys after freeze")
		dumpLinkSet(t, children)
	}

	children.Add("four")
	children.Remove("one")

	links.Freeze()
	var links2 = links.NewVersion()
	var children2 = links2.Link("children")

	if !compareSets(children.ReadKeys(), []string{ "three", "four" }) {
		t.Error("Wrong keys after freeze: %v", children.ReadKeys())
		dumpLinkSet(t, children)
	}

	children2.Add("six")

	if !compareSets(children.ReadKeys(), []string{ "three", "four" }) {
		t.Error("Wrong keys after new version: %v", children.ReadKeys())
		dumpLinkSet(t, children)
	}

	if !compareSets(children2.ReadKeys(), []string{ "three", "four", "six" }) {
		dumpLinkSet(t, children2)
		t.Error("Wrong keys in new version: %v", children2.ReadKeys())
		dumpLinkSet(t, children)
	}

	if len(children2.Previous) != 2 {
		t.Errorf("Wrong key count in copied previous: %d (%v)\n", 
			len(children2.Previous), children2.ReadKeys())
	}

	if len(children2.Current) != 1 {
		t.Errorf("Wrong key count in copied current: %d (%v)\n", 
			len(children2.Previous), children2.ReadKeys())
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
	t.Log("------- Previous ---------\n")
	for k, v := range ls.Previous {
		t.Logf("%s => %v\n", k, v)
	}
	t.Log("------- Current ---------\n")
	for k, v := range ls.Current {
		t.Logf("%s => %v\n", k, v)
	}
}