package loge

import (
	"reflect"
	"sync/atomic"
	"runtime"
)

type Logeable interface {
	TypeName() string
	Key() string
}

type LockState int32
const (
	UNLOCKED = 0
	LOCKED = 1
)

type LogeObject struct {
	DB *LogeDB
	Type *LogeType
	Key string
	Locked int32
	Current *LogeObjectVersion
}

type LogeObjectVersion struct {
	Version int
	TransactionCount int
	Object interface{}
	Previous *LogeObjectVersion
}


func InitializeObject(key string, db *LogeDB, t *LogeType) *LogeObject {

	var obj = reflect.Zero(reflect.TypeOf(t.Exemplar)).Interface()

	return &LogeObject{
		DB: db,
		Type: t,
		Key: key,
		Locked: 0,
		Current: &LogeObjectVersion{
			Version: 0,
			Previous: nil,
			TransactionCount: 0,
			Object: obj,
		},
	}
}


func (obj *LogeObject) NewVersion() *LogeObjectVersion {
	return &LogeObjectVersion{
		Version: obj.Current.Version + 1,
		Previous: obj.Current,
		TransactionCount: 0,
		Object: copyObject(obj.Current.Object),
	}
}


func (obj *LogeObject) Ensure() *LogeObject {
	if obj.Current.Version == 0 {
		obj = obj.DB.EnsureObj(obj)
	}
	return obj
}


func (obj *LogeObject) Applicable(version *LogeObjectVersion) bool {
	return version.Version == obj.Current.Version + 1
}


func (obj *LogeObject) ApplyVersion(version *LogeObjectVersion) {
	version.Previous = obj.Current
	obj.Current = version
}


func (obj *LogeObject) TryLock() bool {
	return atomic.CompareAndSwapInt32(
		&obj.Locked, UNLOCKED, LOCKED)
}

func (obj *LogeObject) SpinLock() {
	for {
		if obj.TryLock() {
			return
		}
		runtime.Gosched()
	}
}

func (obj *LogeObject) Unlock() {
	obj.Locked = UNLOCKED
}


func copyObject(object interface{}) interface{} {
	var value = reflect.ValueOf(object)

	if value.IsNil() {
		return object
	}

	var orig = value.Elem()
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



