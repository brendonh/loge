package loge

type LogeStore interface {
	get(t *logeType, key LogeKey) interface{}
	getLinks(*logeType, string, LogeKey) []string
	find(*logeType, string, LogeKey) ResultSet
	close()

	registerType(*logeType)
	newWriteBatch() writeBatch
}

type ResultSet interface {
	Next() LogeKey
	Valid() bool
	Close()
}

type writeBatch interface {
	Store(*logeObject) error
	StoreLinks(*logeObject) error
	Commit() error
}


type objectMap map[string]map[LogeKey]interface{}


type memStore struct {
	objects objectMap
	lock spinLock
}

type memWriteBatch struct {
	store *memStore
	writes []memWriteEntry
}

type memWriteEntry struct {
	TypeKey string
	ObjKey LogeKey
	Value interface{}
}

func NewMemStore() LogeStore {
	return &memStore{
		objects: make(objectMap),
	}
}

func (store *memStore) close() {
}

func (store *memStore) registerType(typ *logeType) {
	store.objects[typ.Name] = make(map[LogeKey]interface{})
	for linkName := range typ.Links {
		var lk = memStoreLinkKey(typ.Name, linkName)
		store.objects[lk] = make(map[LogeKey]interface{})
	}
}

func (store *memStore) get(t *logeType, key LogeKey) interface{} {
	var objMap = store.objects[t.Name]

	object, ok := objMap[key]
	if !ok {
		return nil
	}

	return object
}

func (store *memStore) getLinks(typ *logeType, linkName string, key LogeKey) []string {
	var lk = memStoreLinkKey(typ.Name, linkName)
	links, ok := store.objects[lk][key]
	if ok {
		return links.(linkList)
	}

	return linkList{}
}

func (store *memStore) find(typ *logeType, linkName string, key LogeKey) ResultSet {
	// Until I can be bothered
	panic("Find not implemented on memstore")
}

func (store *memStore) newWriteBatch() writeBatch {
	return &memWriteBatch{
		store: store,
	}
}


func (batch *memWriteBatch) Store(obj *logeObject) error {
	batch.writes = append(
		batch.writes,
		memWriteEntry{ 
		TypeKey: obj.Type.Name, 
		ObjKey: obj.Key, 
		Value: obj.Current.Object,
	})
	return nil
}

func (batch *memWriteBatch) StoreLinks(obj *logeObject) error {
	batch.writes = append(
		batch.writes,
		memWriteEntry{ 
		TypeKey: memStoreLinkKey(obj.Type.Name, obj.LinkName),
		ObjKey: obj.Key,
		Value: linkList(obj.Current.Object.(*linkSet).ReadKeys()),
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