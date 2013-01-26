package loge

import (
	"sync"
	"reflect"
)

type objCache map[LogeKey]*LogeObject

type LogeType struct {
	Name string
	Version uint16
	Exemplar interface{}
	LinkSpec LinkSpec
	Mutex sync.Mutex
	Cache objCache
}


func (t *LogeType) NewLinks() *ObjectLinks {
	return NewObjectLinks(t.LinkSpec)
}

func (t *LogeType) NilValue() interface{} {
	return reflect.Zero(reflect.TypeOf(t.Exemplar)).Interface()
}


func (t *LogeType) Copy(object interface{}) interface{} {
	var value = reflect.ValueOf(object)

	if value.IsNil() {
		return object
	}

	var orig = value.Elem()
	var val = reflect.New(orig.Type()).Elem()
	val.Set(orig)

	var vt = val.Type()
	for i := 0; i < val.NumField(); i++ {

		var field = val.Field(i)
		var ft = vt.Field(i)

		switch field.Kind() {
		case reflect.Array, 
			reflect.Slice:

			switch ft.Tag.Get("loge") {
			case "copy":
				var newField = reflect.New(field.Type()).Elem()
				newField = reflect.AppendSlice(newField, field)
				field.Set(newField)
			case "keep":
				// Do nothing
			default:
				field.Set(reflect.New(field.Type()).Elem())
			}
		}
	}

	return val.Addr().Interface()
}