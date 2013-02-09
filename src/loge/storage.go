package loge


type LogeStore interface {
	storeContext
	close()
	registerType(*logeType)
	newContext() transactionContext
}

type ResultSet interface {
	All() []LogeKey
	Next() LogeKey
	Valid() bool
	Close()
}

type storeContext interface {
	get(*logeType, LogeKey) interface{}
	getLinks(*logeType, string, LogeKey) []string
	store(*logeObject) error
	storeLinks(*logeObject) error

	find(*logeType, string, LogeKey) ResultSet
	findFrom(*logeType, string, LogeKey, LogeKey, int) ResultSet
}

type transactionContext interface {
	storeContext
	commit() error
	rollback()
}

type objectMap map[string]map[LogeKey]interface{}


type memStore struct {
	objects objectMap
	lock spinLock
}

type memContext struct {
	mstore *memStore
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

func (store *memStore) findFrom(typ *logeType, linkName string, key LogeKey, from LogeKey, limit int) ResultSet {
	// Until I can be bothered
	panic("Find not implemented on memstore")
}

func (store *memStore) store(obj *logeObject) error {
	obj.Lock.SpinLock()
	defer obj.Lock.Unlock()
	if !obj.Current.hasValue() {
		delete(store.objects[obj.Type.Name], obj.Key)
	} else {
		store.objects[obj.Type.Name][obj.Key] = obj.Current.Object
	}
	return nil
}

func (store *memStore) storeLinks(obj *logeObject) error {
	obj.Lock.SpinLock()
	defer obj.Lock.Unlock()
	var typeKey = memStoreLinkKey(obj.Type.Name, obj.LinkName)
	var val = linkList(obj.Current.Object.(*linkSet).ReadKeys())
	store.objects[typeKey][obj.Key] = val
	return nil
}

func (store *memStore) newContext() transactionContext {
	return &memContext{
		mstore: store,
	}
}


func (context *memContext) get(t *logeType, key LogeKey) interface{} {
	return context.mstore.get(t, key)
}

func (context *memContext) getLinks(t *logeType, linkName string, key LogeKey) []string {
	return context.mstore.getLinks(t, linkName, key)
}

func (context *memContext) store(obj *logeObject) error {
	var val interface{}
	if !obj.Current.hasValue() {
		val = nil
	} else {
		val = obj.Current.Object
	}
	context.writes = append(
		context.writes,
		memWriteEntry{ 
		TypeKey: obj.Type.Name, 
		ObjKey: obj.Key, 
		Value: val,
	})
	return nil
}

func (context *memContext) storeLinks(obj *logeObject) error {
	context.writes = append(
		context.writes,
		memWriteEntry{ 
		TypeKey: memStoreLinkKey(obj.Type.Name, obj.LinkName),
		ObjKey: obj.Key,
		Value: linkList(obj.Current.Object.(*linkSet).ReadKeys()),
	})
	return nil
}

func (context *memContext) find(typ *logeType, linkName string, key LogeKey) ResultSet {
	// Until I can be bothered
	panic("Find not implemented on memstore")
}

func (context *memContext) findFrom(typ *logeType, linkName string, key LogeKey, from LogeKey, limit int) ResultSet {
	// Until I can be bothered
	panic("Find not implemented on memstore")
}

func (context *memContext) commit() error {
	var store = context.mstore
	store.lock.SpinLock()
	defer store.lock.Unlock()
	for _, entry := range context.writes {
		store.objects[entry.TypeKey][entry.ObjKey] = entry.Value
	}
	return nil
}

func (context *memContext) rollback() {
}


func memStoreLinkKey(typeName string, linkName string) string {
	return "^" + typeName + "^" + linkName
}