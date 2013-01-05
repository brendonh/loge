package loge

import (
	"reflect"
)


type StringType struct {
}

func (t *StringType) NilValue() interface{} {
	return nil
}

func (t *StringType) Encode(val interface{}) []byte {
	return []byte(val.(string))
}

func (t *StringType) Decode(enc []byte) interface{} {
	return string(enc)
}

func (t *StringType) Copy(val interface{}) interface{} {
	return val
}


// ------------------------------------

type StructType struct {
	Exemplar interface{}
}

func StructTypeFor(exemplar interface{}) *StructType {
	return &StructType{
		Exemplar: exemplar,
	}
}

func (t *StructType) NilValue() interface{} {
	return reflect.Zero(reflect.TypeOf(t.Exemplar)).Interface()
}

// XXX TODO
func (t *StructType) Encode(val interface{}) []byte {
	return []byte{}
}

// XXX TODO
func (t *StructType) Decode(enc []byte) interface{} {
	return t.NilValue()
}

func (t *StructType) Copy(object interface{}) interface{} {
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