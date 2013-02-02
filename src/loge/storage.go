package loge

type LogeStore interface {
	RegisterType(*LogeType)

	Store(*LogeObject) error
	Get(t *LogeType, key LogeKey) interface{}

	StoreLinks(*LogeObject) error
	GetLinks(*LogeType, string, LogeKey) Links
}


type objectMap map[string]map[LogeKey]interface{}


type MemStore struct {
	objects objectMap
}


func NewMemStore() *MemStore {
	return &MemStore{
		objects: make(objectMap),
	}
}


func (store *MemStore) RegisterType(typ *LogeType) {
	store.objects[typ.Name] = make(map[LogeKey]interface{})
	for linkName := range typ.LinkSpec {
		var lk = store.linkKey(typ.Name, linkName)
		store.objects[lk] = make(map[LogeKey]interface{})
	}
}


func (store *MemStore) Store(obj *LogeObject) error {
	store.objects[obj.Type.Name][obj.Key] = obj.Current.Object
	return nil
}


func (store *MemStore) Get(t *LogeType, key LogeKey) interface{} {
	var objMap = store.objects[t.Name]

	object, ok := objMap[key]
	if !ok {
		return nil
	}

	return object
}


func (store *MemStore) StoreLinks(obj *LogeObject) error {
	var lk = store.linkKey(obj.Type.Name, obj.LinkName)
	store.objects[lk][obj.Key] = Links(obj.Current.Object.(*LinkSet).ReadKeys())
	return nil
}

func (store *MemStore) GetLinks(typ *LogeType, linkName string, key LogeKey) Links {
	var lk = store.linkKey(typ.Name, linkName)
	links, ok := store.objects[lk][key]
	if ok {
		return links.(Links)
	}

	return Links{}
}

func (store *MemStore) linkKey(typeName string, linkName string) string {
	return "^" + typeName + "^" + linkName
}