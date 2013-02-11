package loge

import (
	"testing"
	"sort"
)

func TestLinks(t *testing.T) {

	var children = newLinkSet()

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

func TestLinkStorage(test *testing.T) {
	var db = NewLogeDB(NewMemStore())
 
	var def = NewTypeDef("test", 1, &TestObj{})
	def.Links = LinkSpec{ "sibling": "test" }
	db.CreateType(def)

	db.Transact(func (t *Transaction) {
		var links = t.ReadLinks("test", "sibling", "one")
		if len(links) != 0 {
			test.Errorf("Links not empty: %v", links)
		}
		
		t.AddLink("test", "sibling", "one", "two")

		links = t.ReadLinks("test", "sibling", "one")
		if len(links) != 1 || links[0] != "two" {
			test.Errorf("Links not [two]: %v", links)
		}
	}, 0)

	db.Transact(func (t *Transaction) {
		var links = t.ReadLinks("test", "sibling", "one")
		if len(links) != 1 || links[0] != "two" {
			test.Errorf("Links not [two]: %v", links)
		}
	}, 0)	
}


func TestLinkScoping(test *testing.T) {
	var db = NewLogeDB(NewMemStore())

	var def = NewTypeDef("test", 1, &TestObj{})
	def.Links = LinkSpec{ "sibling": "test" }
	db.CreateType(def)

	var trans1 = db.CreateTransaction()
	var trans2 = db.CreateTransaction()

	trans1.AddLink("test", "sibling", "one", "one")

	if trans2.HasLink("test", "sibling", "one", "one") {
		test.Errorf("Link scope leak: %v", trans2.ReadLinks("test", "sibling", "one"));
	}

	if !trans1.Commit() {
		test.Error("Commit failed")
	}

	db.Transact(func (t *Transaction) {
		var links3 = t.ReadLinks("test", "sibling", "one")
		if len(links3) != 1 || links3[0] != "one" {
			test.Errorf("Wrong links after commit: %v", links3)
		}
	}, 0)
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

func dumplinkSet(t *testing.T, ls *linkSet) {
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