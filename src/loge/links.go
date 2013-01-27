package loge

import (
	_ "fmt"
	"sort"
)


type Links []string

func (links Links) Has(key string) bool {
	var i = sort.SearchStrings(links, key)
	return i < len(links) && links[i] == key
}

func (links Links) Add(key string) Links {
	if links.Has(key) {
		return links
	}

	var newLinks = append(links, key)
	sort.Strings(newLinks)
	return newLinks
}

func (links Links) Remove(key string) Links {
	var i = sort.SearchStrings(links, key)
	if i >= len(links) || links[i] != key {
		return links
	}
	return append(links[:i], links[i+1:]...)
}


type ObjectLinks struct {
	sets map[string]*LinkSet
}


type LinkSet struct {
	Name string
	TypeName string
	Original Links
	Added Links
	Removed Links
	Loaded bool
}


type LinkSpec map[string]string


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
	}
}


func (ol ObjectLinks) Link(setName string) *LinkSet {
	var set = ol.sets[setName]
	if !set.Loaded {
		// TODO: Load from DB
	}
	return set
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
		Original: ls.Original,
		Loaded: ls.Loaded,
	}
}

func (ls *LinkSet) Freeze() {
	ls.Original = ls.ReadKeys()
	ls.Added = nil
	ls.Removed = nil
}



func (ls *LinkSet) Set(keys []string) {
	sort.Strings(keys)
	ls.Removed = ls.Original
	ls.Added = keys
}


func (ls *LinkSet) Add(key string) {
	ls.Removed = ls.Removed.Remove(key)
	ls.Added = ls.Added.Add(key)
}

func (ls *LinkSet) Remove(key string) {
	ls.Added = ls.Added.Remove(key)
	ls.Removed = ls.Removed.Add(key)
}

func (ls *LinkSet) ReadKeys() []string {
	var keys []string

	for _, key := range ls.Original {
		if !ls.Removed.Has(key) {
			keys = append(keys, key)
		}
	}

	keys = append(keys, ls.Added...)

	sort.Strings(keys)
	return keys
}

func (ls *LinkSet) Has(key string) bool {
	if ls.Removed.Has(key) {
		return false;
	}
	
	return ls.Added.Has(key) || ls.Original.Has(key)
}

