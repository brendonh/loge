package loge

import (
	"reflect"
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
	RefCount uint32
	LinkName string
	Lock SpinLock
	Loaded bool
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
		RefCount: 0,
		Loaded: false,
	}
}


func (obj *LogeObject) NewVersion() *LogeObjectVersion {
	var current = obj.Current

	var newObj = obj.Type.Copy(current.Object)

	return &LogeObjectVersion{
		LogeObj: obj,
		Version: current.Version + 1,
		Object: newObj,
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



func (version *LogeObjectVersion) HasValue() bool {
	var value = reflect.ValueOf(version.Object)
	return !value.IsNil()
}

