package loge

import (
	"reflect"
	"sync/atomic"
	"runtime"
)

const (
	UNLOCKED = 0
	LOCKED = 1
)

type LogeObject struct {
	DB *LogeDB
	Type *LogeType
	Key LogeKey
	Current *LogeObjectVersion
	Locked int32
	RefCount uint32
}

type LogeObjectVersion struct {
	Version int
	Object interface{}
	Links *ObjectLinks
	Previous *LogeObjectVersion
}


func InitializeObject(db *LogeDB, t *LogeType, key LogeKey, version *LogeObjectVersion) *LogeObject {
	return &LogeObject{
		DB: db,
		Type: t,
		Key: key,
		Current: version,
		Locked: 0,
		RefCount: 0,
	}
}


func (obj *LogeObject) NewVersion() *LogeObjectVersion {
	var current = obj.Current
	return &LogeObjectVersion{
		Version: current.Version + 1,
		Previous: current,
		Object: obj.Type.Copy(current.Object),
		Links: current.Links.NewVersion(),
	}
}


func (obj *LogeObject) Applicable(version *LogeObjectVersion) bool {
	return version.Version == obj.Current.Version + 1
}


func (obj *LogeObject) ApplyVersion(version *LogeObjectVersion) {
	version.Links.Freeze()
	version.Previous = obj.Current
	obj.Current = version
	obj.DB.StoreObj(obj)
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

func (version *LogeObjectVersion) HasValue() bool {
	var value = reflect.ValueOf(version.Object)
	return !value.IsNil()
}