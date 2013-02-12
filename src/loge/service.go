package loge

import (
	"fmt"

	. "github.com/brendonh/go-service"
)

type LogeServiceContext struct {
	Server
	DB *LogeDB
}

func GetService() *Service {
	service := NewService("loge")
	service.AddMethod(
		"info",
		[]APIArg {},
		method_info)
	service.AddMethod(
		"find",
		[]APIArg {
		  APIArg{Name: "type", ArgType: StringArg},
		  APIArg{Name: "linkName", ArgType: StringArg},
		  APIArg{Name: "target", ArgType: StringArg},
 		  APIArg{Name: "from", ArgType: StringArg, Default: ""},
		  APIArg{Name: "limit", ArgType: UIntArg, Default: -1},
	    },
		method_find)
	service.AddMethod(
		"get",
		[]APIArg {
		  APIArg{Name: "type", ArgType: StringArg},
		  APIArg{Name: "key", ArgType: StringArg},
	    },
		method_get)

	return service
}

func method_info(args APIData, session Session, context ServerContext) (bool, APIData) {
	var db = context.(*LogeServiceContext).DB

	var dbInfo string
	switch db.store.(type) {
	case *memStore:
		dbInfo = "Memory"
	case *levelDBStore:
		dbInfo = fmt.Sprintf("LevelDB: %s", db.store.(*levelDBStore).basePath)
	}

	var types []string
	for typeName := range db.types {
		types = append(types, typeName)
	}

	var response = make(APIData)
	response["DB"] = dbInfo
	response["Types"] = types
	return true, response
}

func method_find(args APIData, session Session, context ServerContext) (bool, APIData) {
	var db = context.(*LogeServiceContext).DB

	var response = make(APIData)
	response["keys"] = db.FindSlice(
		args["type"].(string),
		args["linkName"].(string),
		LogeKey(args["target"].(string)),
		LogeKey(args["from"].(string)),
		args["limit"].(int))
	return true, response
}

func method_get(args APIData, session Session, context ServerContext) (bool, APIData) {
	var db = context.(*LogeServiceContext).DB
	var response = make(APIData)

	var typeName = args["type"].(string)
	var key = LogeKey(args["key"].(string))

	var obj interface{}
	var links  = make(map[string][]string)
	db.TransactJSON(func (t *Transaction) {
		obj = t.Read(typeName, key)
		if obj != nil {
			for linkName := range db.types[typeName].Links {
				links[linkName] = t.ReadLinks(typeName, linkName, key)
			}
		}
	}, 0)

	if obj == nil {
		response["found"] = false
	} else {
		response["found"] = true
		response["obj"] = obj
		response["links"] = links
	}
	return true, response
}	
