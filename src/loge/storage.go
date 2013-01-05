package loge


type LogeStore interface {
	RegisterType(*LogeType)

	Store(*LogeObject) error
	Get(t *LogeType, key LogeKey) *LogeObjectVersion
}


type objectMap map[string]map[LogeKey]*LogeObjectVersion


type MemStore struct {
	objects objectMap
}


func NewMemStore() *MemStore {
	return &MemStore{
		objects: make(objectMap),
	}
}


func (store *MemStore) RegisterType(typ *LogeType) {
	store.objects[typ.Name] = make(map[LogeKey]*LogeObjectVersion)
}


func (store *MemStore) Store(obj *LogeObject) error {
	store.objects[obj.Type.Name][obj.Key] = obj.Current
	return nil
}


func (store *MemStore) Get(t *LogeType, key LogeKey) *LogeObjectVersion {
	var objMap = store.objects[t.Name]

	version, ok := objMap[key]
	if !ok {
		return nil
	}

	return version
}