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
	Object interface{}
	Dirty bool
	snapshotID uint64
	Previous *objectVersion
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

func (obj *logeObject) getTransactionVersion(sID uint64) *objectVersion {
	var version = obj.Current
	for version.snapshotID > sID {
		version = version.Previous
		if version == nil {
			panic("Couldn't find version")
		}
	}
	return version
}

func (obj *logeObject) newVersion(sID uint64) *objectVersion {
	var current = obj.getTransactionVersion(sID)

	var newObj = obj.Type.Copy(current.Object)

	return &objectVersion{
		LogeObj: obj,
		Object: newObj,
		Dirty: true,
		Previous: obj.Current,
		snapshotID: current.snapshotID,
	}
}

func (obj *logeObject) applyVersion(version *objectVersion, context storeContext, sID uint64) {
	version.snapshotID = sID
	obj.Current = version
	obj.Loaded = true

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

