package loge

import (
	"reflect"
)

type LogeType struct {
	Name string
	Version int
	Exemplar interface{}
	LinkSpec LinkSpec
}


func (t *LogeType) NilValue() interface{} {
	return reflect.Zero(reflect.TypeOf(t.Exemplar)).Interface()
}


func (t *LogeType) NewLinks() *ObjectLinks {
	return NewObjectLinks(t.LinkSpec)
}