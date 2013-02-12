package main

import (
	"fmt"
	"os"
	"os/signal"

	"loge"

	"github.com/brendonh/go-service"
)


func StartService() {
	var db = loge.NewLogeDB(loge.NewLevelDBStore("data/links"))
	defer db.Close()

	db.CreateType(loge.NewTypeDef("person", 1, &Person{}))

	var petDef = loge.NewTypeDef("pet", 1, &Pet{})
	petDef.Links = loge.LinkSpec{ "owner": "person" }
	db.CreateType(petDef)

	var serviceCollection = goservice.NewServiceCollection()
	serviceCollection.AddService(loge.GetService())

	var server = &loge.LogeServiceContext{
		*goservice.NewServer(
			serviceCollection,
			goservice.BasicSessionCreator),
		db,
	}

	server.AddEndpoint(goservice.NewHttpRpcEndpoint(":6060", server, nil))
	server.AddEndpoint(goservice.NewTelnetEndpoint(":6061", server))

	server.Log("Server starting...")

	var stopper = make(chan os.Signal, 1)
	signal.Notify(stopper)

	server.Start()
	defer server.Stop()

	<-stopper
	close(stopper)

	fmt.Printf("\n")
	server.Log("Server stopping...")
}