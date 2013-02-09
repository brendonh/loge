package loge

import (
	"reflect"
)

type logeType struct {
	Name string
	Version uint16
	Exemplar interface{}
	Links map[string]*linkInfo
}


func (t *logeType) NilValue() interface{} {
	return reflect.Zero(reflect.TypeOf(t.Exemplar)).Interface()
}


// XXX TODO: Do this via the store instead, and just re-decode spack objects for consistency
func (t *logeType) Copy(object interface{}) interface{} {
	var value = reflect.ValueOf(object)

	if value.Kind() != reflect.Ptr || reflect.Indirect(value).Kind() != reflect.Struct {
		return object
	}

	if !value.IsValid() || value.IsNil() {
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