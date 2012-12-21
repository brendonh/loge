package loge

import (
	_ "fmt"
)


type ObjectLinks struct {
	sets map[string]*LinkSet
}

type LinkSet struct {
	Name string
	TypeName string
	Previous map[string]bool
	Current map[string]bool
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
		TypeName: typeName,
		Previous: make(Links),
		Current: make(Links),
	}
}


func (ol ObjectLinks) Link(set string) *LinkSet {
	return ol.sets[set]
}

func (ol ObjectLinks) NewVersion() *ObjectLinks {
	var nol = &ObjectLinks {
		sets: make(map[string]*LinkSet),
	}

	for name, set := range ol.sets {
		nol.sets[name] = set.NewVersion()
	}

	return nol
}

func (ol ObjectLinks) Freeze() {
	for _, set := range ol.sets {
		set.Freeze()
	}
}



func (ls *LinkSet) NewVersion() *LinkSet {
	return &LinkSet{
		Name: ls.Name,
		TypeName: ls.TypeName,
		Previous: ls.Current,
		Current: make(Links),
	}
}

func (ls *LinkSet) Freeze() {
	for k, has := range ls.Current {
		if has {
			ls.Previous[k] = true
		} else {
			delete(ls.Previous, k)
		}
	}

	ls.Current = ls.Previous
	ls.Previous = make(Links)
}



func (ls *LinkSet) Set(keys []string) {
	ls.Current = make(Links)
	for _, key := range keys {
		ls.Current[key] = true
	}
}


func (ls *LinkSet) Add(key string) {
	ls.Current[key] = true
}

func (ls *LinkSet) Remove(key string) {
	ls.Current[key] = false
}

func (ls *LinkSet) ReadKeys() []string {
	var roughLen = len(ls.Current) + len(ls.Previous)

	var keys = make([]string, 0, roughLen)
	var deleted = make(Links)

	for k, has := range ls.Current {
		if has {
			keys = append(keys, k)
		} else {
			deleted[k] = true;
		}
	}

	for k, has := range ls.Previous {
		if has {
			_, del := deleted[k]
			if !del {
				keys = append(keys, k)
			}
		}
	}

	return keys
}

func (ls *LinkSet) Has(key string) bool {
	has, ok := ls.Current[key]
	if ok {
		return has
	}

	has, ok = ls.Previous[key]
	return ok && has;
}

