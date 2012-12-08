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

	links.Add("children", "one")
	links.Add("children", "two")

	if !compareSets(links.Read("children"), []string{"one", "two"}) {
		t.Error("Wrong keys after adds: %v", links.Read("children"))
	}

	links.Set("children", []string{"three", "four"})

	if !compareSets(links.Read("children"), []string{"three", "four"}) {
		t.Error("Wrong keys after set: %v", links.Read("children"))
	}

	links.Add("children", "five")

	if !compareSets(links.Read("children"), []string{"three", "four", "five"}) {
		t.Error("Wrong keys after set+add: %v", links.Read("children"))
	}
	
	if !links.Has("children", "four") {
		t.Error("Key four missing")
	}

	if links.Has("children", "one") {
		t.Error("Key one present")
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