package loge

import (
	"reflect"
	"fmt"

	"github.com/brendonh/spack"
)

var linkSpec *spack.TypeSpec = spack.MakeTypeSpec([]string{})
var linkInfoSpec *spack.TypeSpec = spack.MakeTypeSpec(linkInfo{})

type logeType struct {
	Name string
	Version uint16
	Exemplar interface{}
	SpackType *spack.VersionedType
	Links map[string]*linkInfo
}

func NewType(name string, version uint16, exemplar interface{}, linkSpec LinkSpec, spackType *spack.VersionedType) *logeType {
	var infos = make(map[string]*linkInfo)
	for k, v := range linkSpec {
		infos[k] = &linkInfo{
			Name: k,
			Target: v,
			Tag: 1,
		}
	}

	return &logeType {
		Name: name,
		Version: version,
		Exemplar: exemplar,
		SpackType: spackType,
		Links: infos,
	}
}

func (t *logeType) NilValue() interface{} {
	return reflect.Zero(reflect.TypeOf(t.Exemplar)).Interface()
}

func (t *logeType) Decode(enc []byte) interface{} {
	if len(enc) == 0 {
		return t.NilValue()
	}
	
	obj, err := t.SpackType.DecodeObj(enc)
	if err != nil {
		panic(fmt.Sprintf("Decode error: %v", err))
	}
	return obj
}

func (t *logeType) Encode(obj interface{}) []byte {
	enc, err := t.SpackType.EncodeObj(obj)
	if err != nil {
		panic(fmt.Sprintf("Encode error: %v", err))
	}
	return enc
}
