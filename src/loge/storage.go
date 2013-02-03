package loge

type LogeStore interface {
	Close()

	RegisterType(*LogeType)

	Get(t *LogeType, key LogeKey) interface{}
	GetLinks(*LogeType, string, LogeKey) Links

	NewWriteBatch() LogeWriteBatch
}

type LogeWriteBatch interface {
	Store(*LogeObject) error
	StoreLinks(*LogeObject) error
	Commit() error
}


type objectMap map[string]map[LogeKey]interface{}


type MemStore struct {
	objects objectMap
	lock SpinLock
}

type MemStoreWriteBatch struct {
	store *MemStore
	writes []MemStoreWriteEntry
}

type MemStoreWriteEntry struct {
	TypeKey string
	ObjKey LogeKey
	Value interface{}
}

func NewMemStore() *MemStore {
	return &MemStore{
		objects: make(objectMap),
	}
}

func (store *MemStore) Close() {
}

func (store *MemStore) RegisterType(typ *LogeType) {
	store.objects[typ.Name] = make(map[LogeKey]interface{})
	for linkName := range typ.Links {
		var lk = memStoreLinkKey(typ.Name, linkName)
		store.objects[lk] = make(map[LogeKey]interface{})
	}
}

func (store *MemStore) Get(t *LogeType, key LogeKey) interface{} {
	var objMap = store.objects[t.Name]

	object, ok := objMap[key]
	if !ok {
		return nil
	}

	return object
}

func (store *MemStore) GetLinks(typ *LogeType, linkName string, key LogeKey) Links {
	var lk = memStoreLinkKey(typ.Name, linkName)
	links, ok := store.objects[lk][key]
	if ok {
		return links.(Links)
	}

	return Links{}
}


func (store *MemStore) NewWriteBatch() LogeWriteBatch {
	return &MemStoreWriteBatch{
		store: store,
	}
}


func (batch *MemStoreWriteBatch) Store(obj *LogeObject) error {
	batch.writes = append(
		batch.writes,
		MemStoreWriteEntry{ 
		TypeKey: obj.Type.Name, 
		ObjKey: obj.Key, 
		Value: obj.Current.Object,
	})
	return nil
}

func (batch *MemStoreWriteBatch) StoreLinks(obj *LogeObject) error {
	batch.writes = append(
		batch.writes,
		MemStoreWriteEntry{ 
		TypeKey: memStoreLinkKey(obj.Type.Name, obj.LinkName),
		ObjKey: obj.Key,
		Value: Links(obj.Current.Object.(*LinkSet).ReadKeys()),
	})
	return nil
}

func (batch *MemStoreWriteBatch) Commit() error {
	batch.store.lock.SpinLock()
	defer batch.store.lock.Unlock()
	for _, entry := range batch.writes {
		batch.store.objects[entry.TypeKey][entry.ObjKey] = entry.Value
	}
	return nil
}


func memStoreLinkKey(typeName string, linkName string) string {
	return "^" + typeName + "^" + linkName
}