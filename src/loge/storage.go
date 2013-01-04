package loge


type LogeStore interface {
	RegisterType(*LogeType)

	Store(*LogeObject) error
	Get(t *LogeType, key string) *LogeObject
}


type LogeObjectMap map[string]map[string]*LogeObject


type MemStore struct {
	objects LogeObjectMap
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