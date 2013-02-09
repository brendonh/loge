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
	Type *logeType
	Key LogeKey
	Current *objectVersion
	RefCount uint32
	LinkName string
	Lock spinLock
	Loaded bool
}

type objectVersion struct {
	LogeObj *LogeObject
	Version int
	Object interface{}
	Dirty bool
}


func initializeObject(db *LogeDB, t *logeType, key LogeKey) *LogeObject {
	return &LogeObject{
		DB: db,
		Type: t,
		Key: key,
		Current: nil,
		RefCount: 0,
		Loaded: false,
	}
}


func (obj *LogeObject) newVersion() *objectVersion {
	var current = obj.Current

	var newObj = obj.Type.Copy(current.Object)

	return &objectVersion{
		LogeObj: obj,
		Version: current.Version + 1,
		Object: newObj,
		Dirty: true,
	}
}

func (obj *LogeObject) applyVersion(version *objectVersion, batch writeBatch) {
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

func (version *objectVersion) hasValue() bool {
	var value = reflect.ValueOf(version.Object)
	return !value.IsNil()
}

