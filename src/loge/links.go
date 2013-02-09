package loge

import (
	"sort"
)

type linkList []string
type LinkSpec map[string]string

type linkInfo struct {
	Name string
	Target string
	Tag uint16
}

func (links linkList) Has(key string) bool {
	var i = sort.SearchStrings(links, key)
	return i < len(links) && links[i] == key
}

func (links linkList) Add(key string) linkList {
	if links.Has(key) {
		return links
	}

	var newLinks = append(links, key)
	sort.Strings(newLinks)
	return newLinks
}

func (links linkList) Remove(key string) linkList {
	var i = sort.SearchStrings(links, key)
	if i >= len(links) || links[i] != key {
		return links
	}
	return append(links[:i], links[i+1:]...)
}



type LinkSet struct {
	Original linkList `loge:"keep"`
	Added linkList
	Removed linkList
}


func NewLinkSet() *LinkSet {
	return &LinkSet{
	}
}


func (ls *LinkSet) NewVersion() *LinkSet {
	return &LinkSet{
		Original: ls.Original,
	}
}


func (ls *LinkSet) Freeze() {
	ls.Original = ls.ReadKeys()
	ls.Added = nil
	ls.Removed = nil
}



func (ls *LinkSet) Set(keys []string) {
	// XXX BGH TODO: Delta this
	sort.Strings(keys)	
	ls.Removed = ls.Original
	ls.Added = keys
}


func (ls *LinkSet) Add(key string) {
	ls.Removed = ls.Removed.Remove(key)
	if !ls.Original.Has(key) {
		ls.Added = ls.Added.Add(key)
	}
}

func (ls *LinkSet) Remove(key string) {
	// XXX BGH Hrgh
	if (!ls.Original.Has(key) && !ls.Added.Has(key)) || ls.Removed.Has(key) {
		return
	}

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

