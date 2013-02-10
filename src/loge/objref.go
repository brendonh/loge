package loge

type objRef struct {
	TypeName string
	Key LogeKey
	LinkName string
	CacheKey string
}

func makeObjRef(typeName string, key LogeKey) objRef {
	var cacheKey = typeName + "^" + string(key)
	return objRef { typeName, key, "", cacheKey }
}

func makeLinkRef(typeName string, linkName string, key LogeKey) objRef {
	var cacheKey = "^" + typeName + "^" + linkName + "^" + string(key)
	return objRef { typeName, key, linkName, cacheKey }
}

func (objRef objRef) String() string {
	return objRef.CacheKey
}

func (objRef objRef) IsLink() bool {
	return objRef.LinkName != ""
}
