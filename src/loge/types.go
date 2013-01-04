package loge

import (
	"reflect"
	"sync"
)

type objCache map[string]*LogeObject

type LogeType struct {
	Name string
	Version int
	Exemplar interface{}
	LinkSpec LinkSpec
	Mutex sync.Mutex
	Cache objCache
}


func (t *LogeType) NilValue() interface{} {
	return reflect.Zero(reflect.TypeOf(t.Exemplar)).Interface()
}


func (t *LogeType) NewLinks() *ObjectLinks {
	return NewObjectLinks(t.LinkSpec)
}