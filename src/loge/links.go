package loge

import (
	_ "fmt"
)


type ObjectLinks struct {
	sets map[string]*LinkSet
}

type LinkSet struct {
	Name string
	typeName string
	links map[string]bool
	Changed bool
}

type LinkSpec map[string]string

type Links map[string]bool


func NewObjectLinks(spec LinkSpec) *ObjectLinks {
	var ol = &ObjectLinks {
		sets: make(map[string]*LinkSet),
	}
	for set, typeName := range spec {
		ol.sets[set] = NewLinkSet(set, typeName)
	}
	return ol
}


func NewLinkSet(name string, typeName string) *LinkSet {
	return &LinkSet{
		Name: name,
		typeName: typeName,
		links: make(Links),
		Changed: false,
	}
}


func (ol ObjectLinks) Link(set string) *LinkSet {
	return ol.sets[set]
}

func (ol ObjectLinks) Freeze() []*LinkSet {
	var changed = make([]*LinkSet, 0, len(ol.sets))
	for _, linkset := range ol.sets {
		if linkset.Changed {
			changed = append(changed, linkset)
			linkset.Changed = false
		}
	}
	return changed
}

func (ls *LinkSet) Set(keys []string) {
	ls.links = make(Links)
	ls.Changed = true
	for _, key := range keys {
		ls.links[key] = true
	}
}

func (ls *LinkSet) Add(key string) {
	ls.Touch()
	ls.links[key] = true
}

func (ls *LinkSet) Remove(key string) {
	_, ok := ls.links[key]
	if ok {
		ls.Touch()
		delete(ls.links, key)
	}
}

func (ls *LinkSet) ReadKeys() []string {
	var keys = make([]string, 0, len(ls.links))
	for k := range ls.links {
		keys = append(keys, k)
	}
	return keys
}

func (ls *LinkSet) Has(key string) bool {
	_, ok := ls.links[key]
	return ok
}

func (ls *LinkSet) Touch() {
	if !ls.Changed {
		var newLinks = make(Links)
		for k, v := range ls.links {
			newLinks[k] = v
		}
		ls.links = newLinks
		ls.Changed = true
	}
}
