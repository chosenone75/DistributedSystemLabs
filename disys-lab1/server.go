package main

import (
	"fmt"
	"net/http"
	"net/rpc"
	"rpc_objects"
)

func main() {

	st := new(rpc_objects.ServerTimer)
	rpc.Register(st)
	rpc.HandleHTTP()
	fmt.Println("Start listening...")
	err := http.ListenAndServe(":1234", nil)
	if err != nil {
		fmt.Println(err)
	}
}
