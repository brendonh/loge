package loge

import (
	"sync"
)

type objCache map[LogeKey]*LogeObject

type LogeType struct {
	Name string
	Version int
	ObjType LogeObjectType
	LinkSpec LinkSpec
	Mutex sync.Mutex
	Cache objCache
}


func (t *LogeType) NewLinks() *ObjectLinks {
	return NewObjectLinks(t.LinkSpec)
}