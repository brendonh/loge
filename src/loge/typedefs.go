package loge

type LogeKey string

type LogeObjectType interface {
	NilValue() interface{}
	Encode(interface{}) []byte
	Decode([]byte) interface{}
	Copy(interface{}) interface{}
}

