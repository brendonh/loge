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
	LinkName string
}

type LogeObjectVersion struct {
	LogeObj *LogeObject
	Version int
	Object interface{}
	Dirty bool
}


func InitializeObject(db *LogeDB, t *LogeType, key LogeKey) *LogeObject {
	return &LogeObject{
		DB: db,
		Type: t,
		Key: key,
		Current: nil,
		Locked: 0,
		RefCount: 0,
	}
}


func (obj *LogeObject) NewVersion() *LogeObjectVersion {
	var current = obj.Current
	return &LogeObjectVersion{
		LogeObj: obj,
		Version: current.Version + 1,
		Object: obj.Type.Copy(current.Object),
		Dirty: true,
	}
}


func (obj *LogeObject) Applicable(version *LogeObjectVersion) bool {
	return version.Version == obj.Current.Version + 1
}


func (obj *LogeObject) ApplyVersion(version *LogeObjectVersion, batch LogeWriteBatch) {
	obj.Current = version

	if obj.LinkName == "" {
		batch.Store(obj)
	} else {
		batch.StoreLinks(obj)
	}

	version.Dirty = false
	if obj.LinkName != "" {
		version.Object.(*LinkSet).Freeze()
	}
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

