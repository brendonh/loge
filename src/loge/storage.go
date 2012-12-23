package loge

import (
	"sync"
)


type LogeStore interface {
	RegisterType(*LogeType)

	Store(*LogeObject) error
	Get(t *LogeType, key string) *LogeObject
	Ensure (*LogeObject) *LogeObject
}


type LogeObjectMap map[string]map[string]*LogeObject


type MemStore struct {
	objects LogeObjectMap
	mutex sync.Mutex
}


func NewMemStore() *MemStore {
	return &MemStore{
		objects: make(LogeObjectMap),
	}
}


func (store *MemStore) RegisterType(typ *LogeType) {
	store.objects[typ.Name] = make(map[string]*LogeObject)
}


func (store *MemStore) Store(obj *LogeObject) error {
	store.objects[obj.Type.Name][obj.Key] = obj
	return nil
}


func (store *MemStore) Get(t *LogeType, key string) *LogeObject {
	var objMap = store.objects[t.Name]

	obj, ok := objMap[key]
	if !ok {
		return nil
	}

	return obj
}


func (store *MemStore) Ensure(obj *LogeObject) *LogeObject {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	var objMap = store.objects[obj.Type.Name]

	existing, ok := objMap[obj.Key]

	if ok {
		return existing
	}

	objMap[obj.Key] = obj
	return obj
}