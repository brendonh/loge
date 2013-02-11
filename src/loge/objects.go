package loge

import (
	"fmt"
	"reflect"

	"github.com/brendonh/spack"
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
	Blob []byte
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

func (obj *logeObject) makeObjRef() objRef {
	if obj.LinkName != "" {
		return makeLinkRef(obj.Type, obj.LinkName, obj.Key)
	}
	return makeObjRef(obj.Type, obj.Key)
}

func (obj *logeObject) getVersion(sID uint64) *objectVersion {
	var version = obj.Current
	for version.snapshotID > sID {
		version = version.Previous
		if version == nil {
			panic("Couldn't find version")
		}
	}
	return version
}

func (obj *logeObject) applyVersion(object interface{}, context transactionContext, sID uint64) {
	var blob = obj.encode(object)

	obj.Current = &objectVersion{
		LogeObj: obj,
		Blob: blob,
		Previous: obj.Current,
		snapshotID: sID,
	}
	obj.Loaded = true

	var ref = obj.makeObjRef()
	context.store(ref, blob)

	if obj.LinkName != "" {
		var links = object.(*linkSet)
		
		for _, target := range links.Removed {
			context.remIndex(makeLinkRef(obj.Type, obj.LinkName, LogeKey(target)), obj.Key)
		}
		for _, target := range links.Added {
			context.addIndex(makeLinkRef(obj.Type, obj.LinkName, LogeKey(target)), obj.Key)
		}
	}
}

func (obj *logeObject) decode(blob []byte) (object interface{}, upgraded bool) {
	if obj.LinkName == "" {
		object, upgraded = obj.Type.Decode(blob)
	} else {
		var links linkList
		spack.DecodeFromBytes(&links, obj.DB.linkTypeSpec, blob)
		object = &linkSet{ Original: links }
		upgraded = false
	}
	return
}

func (obj *logeObject) encode(object interface{}) []byte {
	if !obj.hasValue(object) {
		return nil
	}

	if obj.LinkName == "" {
		return obj.Type.Encode(object)
	}

	var set = object.(*linkSet)
	enc, err := spack.EncodeToBytes(set.ReadKeys(), obj.DB.linkTypeSpec)
	if err != nil {
		panic(fmt.Sprintf("Link encode error: %v\n", err))
	}
	return enc
}

func (obj *logeObject) hasValue(object interface{}) bool {
	return !reflect.ValueOf(object).IsNil()
}

