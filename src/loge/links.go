package loge

import (
	_ "fmt"
)


type ObjectLinks struct {
	sets map[string]*LinkSet
}

type LinkSet struct {
	typeName string
	links map[string]bool
}


func NewObjectLinks(spec map[string]string) *ObjectLinks {
	var ol = &ObjectLinks {
		sets: make(map[string]*LinkSet),
	}
	for set, typeName := range spec {
		ol.sets[set] = NewLinkSet(typeName)
	}
	return ol
}


func NewLinkSet(typeName string) *LinkSet {
	return &LinkSet{
		typeName: typeName,
		links: make(map[string]bool),
	}
}


func (ol ObjectLinks) Set(set string, keys []string) {
	ol.sets[set].Set(keys)
}

func (ol ObjectLinks) Add(set string, key string) {
	ol.sets[set].Add(key)
}

func (ol ObjectLinks) Read(set string) []string {
	return ol.sets[set].Read()
}

func (ol ObjectLinks) Has(set string, key string) bool {
	return ol.sets[set].Has(key)
}


func (ls *LinkSet) Set(keys []string) {
	ls.links = make(map[string]bool)
	for _, key := range keys {
		ls.links[key] = true
	}
}

func (ls *LinkSet) Add(key string) {
	ls.links[key] = true
}

func (ls *LinkSet) Read() []string {
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