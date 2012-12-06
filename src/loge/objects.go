package loge

import (
	"reflect"
	"sync"
)

type Logeable interface {
	TypeName() string
	Key() string
}

type LogeObject struct {
	DB *LogeDB
	Type *LogeType
	Key string
	Current *LogeObjectVersion
	Mutex sync.Mutex
}

type LogeObjectVersion struct {
	Version int
	TransactionCount int
	Object interface{}
	Previous *LogeObjectVersion
}


func InitializeObject(key string, object interface{}, db *LogeDB, t *LogeType) *LogeObject {

	var obj = &LogeObject{
		DB: db,
		Type: t,
		Key: key,
		Current: &LogeObjectVersion{
			Version: 0,
			Previous: nil,
			TransactionCount: 0,
			Object: object,
		},
	}

	var version = obj.NewVersion()
	version.Object = object
	obj.Current = version
	return obj
}


func (obj *LogeObject) NewVersion() *LogeObjectVersion {
	return &LogeObjectVersion{
		Version: obj.Current.Version + 1,
		Previous: obj.Current,
		TransactionCount: 0,
		Object: copyObject(obj.Current.Object),
	}
}


func (obj *LogeObject) ApplyVersion(version *LogeObjectVersion) bool {
	obj.Mutex.Lock()
	defer obj.Mutex.Unlock()

	if (version.Version != obj.Current.Version + 1) {
		return false
	}

	version.Previous = obj.Current
	obj.Current = version

	return true
}


func copyObject(object interface{}) interface{} {
	var orig = reflect.ValueOf(object).Elem()
	var val = reflect.New(orig.Type()).Elem()
	val.Set(orig)

	var t = val.Type()
	for i := 0; i < val.NumField(); i++ {

		var field = val.Field(i)
		var ft = t.Field(i)

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
				// Empty it
				field.Set(reflect.New(field.Type()).Elem())
			}
		}
	}

	return val.Addr().Interface()
}



