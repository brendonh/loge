package loge

import (
	"reflect"
)

type LogeType struct {
	Name string
	Version int
	Exemplar interface{}
}


func (t *LogeType) NilValue() interface{} {
	return reflect.Zero(reflect.TypeOf(t.Exemplar)).Interface()
}