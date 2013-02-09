package loge

import (
	"reflect"
)

type logeObject struct {
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
	LogeObj *logeObject
	Version int
	Object interface{}
	Dirty bool
}


func initializeObject(db *LogeDB, t *logeType, key LogeKey) *logeObject {
	return &logeObject{
		DB: db,
		Type: t,
		Key: key,
		Current: nil,
		RefCount: 0,
		Loaded: false,
	}
}


func (obj *logeObject) newVersion() *objectVersion {
	var current = obj.Current

	var newObj = obj.Type.Copy(current.Object)

	return &objectVersion{
		LogeObj: obj,
		Version: current.Version + 1,
		Object: newObj,
		Dirty: true,
	}
}

func (obj *logeObject) applyVersion(version *objectVersion, context storeContext) {
	obj.Current = version

	if obj.LinkName == "" {
		context.store(obj)
	} else {
		context.storeLinks(obj)
	}

	version.Dirty = false
	if obj.LinkName != "" {
		version.Object.(*linkSet).Freeze()
	}
}

func (version *objectVersion) hasValue() bool {
	var value = reflect.ValueOf(version.Object)
	return !value.IsNil()
}

