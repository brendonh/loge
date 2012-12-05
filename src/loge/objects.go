package loge

import (
	"reflect"
)

type Logeable interface {
	TypeName() string
	Key() string
}

type LogeObject struct {
	DB *LogeDB
	Type *LogeType
	Key string
	Version int
	Dirty bool
	TransactionCount int
	Object interface{}
}


func (obj *LogeObject) SetOnObject() {
	var val = reflect.ValueOf(obj.Object).Elem()
	var field = val.FieldByName("Loge")
	if !field.IsValid() {
		panic("No Loge attribute on object")
	}

	field.Set(reflect.ValueOf(obj))
}


func (obj LogeObject) Update() interface{} {
	var orig = reflect.ValueOf(obj.Object).Elem()

	var val = reflect.New(orig.Type()).Elem()

	val.Set(orig)

	mungeNested(val)

	var newObject = val.Addr().Interface()
	obj.Object = newObject
	obj.SetOnObject()
	obj.Version++
	return newObject
}


func mungeNested(val reflect.Value) {
	var t = val.Type()
	for i := 0; i < val.NumField(); i++ {

		var field = val.Field(i)
		var ft = t.Field(i)

		switch field.Kind() {
		case reflect.Array, 
			reflect.Slice:
			
			var newField = reflect.New(field.Type()).Elem()
			
			switch ft.Tag.Get("loge") {
			case "copy":
				newField = reflect.AppendSlice(newField, field)
			}

			field.Set(newField)
		}
	}
}



