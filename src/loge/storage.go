package loge

import (
	"sync"
)


type LogeStore interface {
	RegisterType(*LogeType)

	Store(*LogeObject) error
	Get(typeName string, key string) *LogeObject

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
	store.mutex.Lock()
	defer store.mutex.Unlock()

	store.objects[obj.Type.Name][obj.Key] = obj
	return nil
}


func (store *MemStore) Get(typeName string, key string) *LogeObject {
	var objMap = store.objects[typeName]

	obj, ok := objMap[key]
	if !ok {
		return nil
	}

	return obj
}