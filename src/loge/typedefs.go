package loge

type LogeKey string

type LogeObjectType interface {
	NilValue() interface{}
	EncodeMeta() []byte
	Encode(interface{}) []byte
	Decode([]byte, []byte) interface{}
	Copy(interface{}) interface{}
}

