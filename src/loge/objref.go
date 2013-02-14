package loge

import (
	"bytes"
	"encoding/binary"
)

type objRef struct {
	Type *logeType
	Key LogeKey
	LinkName string
	CacheKey string
}

func encodeTypeTag(typ *logeType) uint32 {
	return uint32(typ.SpackType.Tag) << 16
}

func typePrefix(typ *logeType) []byte {
	var buf = bytes.NewBuffer(make([]byte, 0, 4))
	binary.Write(buf, binary.BigEndian, encodeTypeTag(typ))
	return buf.Bytes()
}

func encodeKey(tag uint32, key LogeKey) string {
	var keyBytes = []byte(key)
	var buf = bytes.NewBuffer(make([]byte, 0, len(keyBytes) + 4))
	binary.Write(buf, binary.BigEndian, tag)
	buf.Write(keyBytes)
	return string(buf.Bytes())
}

func makeObjRef(typ *logeType, key LogeKey) objRef {
	var tag = encodeTypeTag(typ)
	var cacheKey = encodeKey(tag, key)
	var ref = objRef{ typ, key, "", cacheKey }
	return ref
}

func makeLinkRef(typ *logeType, linkName string, key LogeKey) objRef {
	var tag = encodeTypeTag(typ) | uint32(typ.Links[linkName].Tag)
	var cacheKey = encodeKey(tag, key)
	var ref = objRef{ typ, key, linkName, cacheKey }
	return ref
}

func (objRef objRef) String() string {
	return objRef.CacheKey
}

func (objRef objRef) IsLink() bool {
	return objRef.LinkName != ""
}
