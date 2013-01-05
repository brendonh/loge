package loge


type LogeStore interface {
	RegisterType(*LogeType)

	Store(*LogeObject) error
	Get(t *LogeType, key LogeKey) *LogeObject
}


type objectMap map[string]map[LogeKey]*LogeObject


type MemStore struct {
	objects objectMap
}


func NewMemStore() *MemStore {
	return &MemStore{
		objects: make(objectMap),
	}
}


func (store *MemStore) RegisterType(typ *LogeType) {
	store.objects[typ.Name] = make(map[LogeKey]*LogeObject)
}


func (store *MemStore) Store(obj *LogeObject) error {
	store.objects[obj.Type.Name][obj.Key] = obj
	return nil
}


func (store *MemStore) Get(t *LogeType, key LogeKey) *LogeObject {
	var objMap = store.objects[t.Name]

	obj, ok := objMap[key]
	if !ok {
		return nil
	}

	return obj
}