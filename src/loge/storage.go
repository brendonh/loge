package loge

type LogeStore interface {
	Get(t *logeType, key LogeKey) interface{}
	GetLinks(*logeType, string, LogeKey) []string
	Find(*logeType, string, LogeKey) ResultSet
	Close()

	registerType(*logeType)
	newWriteBatch() writeBatch
}

type ResultSet interface {
	Next() LogeKey
	Valid() bool
	Close()
}

type writeBatch interface {
	Store(*LogeObject) error
	StoreLinks(*LogeObject) error
	Commit() error
}


type objectMap map[string]map[LogeKey]interface{}


type MemStore struct {
	objects objectMap
	lock spinLock
}

type memWriteBatch struct {
	store *MemStore
	writes []memWriteEntry
}

type memWriteEntry struct {
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

func (store *MemStore) registerType(typ *logeType) {
	store.objects[typ.Name] = make(map[LogeKey]interface{})
	for linkName := range typ.Links {
		var lk = memStoreLinkKey(typ.Name, linkName)
		store.objects[lk] = make(map[LogeKey]interface{})
	}
}

func (store *MemStore) Get(t *logeType, key LogeKey) interface{} {
	var objMap = store.objects[t.Name]

	object, ok := objMap[key]
	if !ok {
		return nil
	}

	return object
}

func (store *MemStore) GetLinks(typ *logeType, linkName string, key LogeKey) []string {
	var lk = memStoreLinkKey(typ.Name, linkName)
	links, ok := store.objects[lk][key]
	if ok {
		return links.(linkList)
	}

	return linkList{}
}

func (store *MemStore) Find(typ *logeType, linkName string, key LogeKey) ResultSet {
	// Until I can be bothered
	panic("Find not implemented on memstore")
}

func (store *MemStore) newWriteBatch() writeBatch {
	return &memWriteBatch{
		store: store,
	}
}


func (batch *memWriteBatch) Store(obj *LogeObject) error {
	batch.writes = append(
		batch.writes,
		memWriteEntry{ 
		TypeKey: obj.Type.Name, 
		ObjKey: obj.Key, 
		Value: obj.Current.Object,
	})
	return nil
}

func (batch *memWriteBatch) StoreLinks(obj *LogeObject) error {
	batch.writes = append(
		batch.writes,
		memWriteEntry{ 
		TypeKey: memStoreLinkKey(obj.Type.Name, obj.LinkName),
		ObjKey: obj.Key,
		Value: linkList(obj.Current.Object.(*LinkSet).ReadKeys()),
	})
	return nil
}

func (batch *memWriteBatch) Commit() error {
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