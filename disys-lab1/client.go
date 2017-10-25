package main

import (
	"fmt"
	"html/template"
	"net/http"
	"net/rpc"
	"os"
	"time"
	"rpc_objects"
	"strconv"
)

type Page struct {
	Title string
}

func testHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "This is a page supported by Golang.")
}

func renderTemplate(w http.ResponseWriter, tmpl string, p *Page) {
	t, _ := template.ParseFiles("clock/" + tmpl + ".html")
	t.Execute(w, p)
}
func timeHandler(w http.ResponseWriter, r *http.Request) {
        p := &Page{Title:"127.0.0.1"}
	renderTemplate(w, "index", p)
}
func ajaxHandler(args *rpc_objects.Args, serveraddr string, client *rpc.Client) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var rv = new(rpc_objects.ReturnValue)
		err := client.Call("ServerTimer.EchoTime", args, &rv)
		checkerror(err, "Calling ServerTimer.EchoTime")
		if rv.Statuscode == 0 {
			fmt.Println("Current Time in " + serveraddr + ":" + rv.Time)
			t,_ := time.Parse("2006-01-02 15:04:05", rv.Time)
			ajaxres := strconv.Itoa(t.Hour()) + " " + strconv.Itoa(t.Minute()) + " " + strconv.Itoa(t.Second())
			fmt.Fprintf(w, ajaxres)
		} else {
			fmt.Println("Authorized Failure, please check your ID and password or REGISTER yourself in server with 'client -R'")
			os.Exit(0)
		}
	}
}
func staticDirHandler(mux *http.ServeMux, prefix string, staticDir string) {
	mux.HandleFunc(prefix, func(w http.ResponseWriter, r *http.Request) {
		file := staticDir + r.URL.Path[len(prefix)-1:]
		http.ServeFile(w, r, file)
	})
}
func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: ", os.Args[0], "server address")
		os.Exit(1)
	}
	serveraddr := os.Args[1]
	args := new(rpc_objects.Args)

	client, err := rpc.DialHTTP("tcp", serveraddr+":1234")
	checkerror(err, "Dialing HTTP")

	if len(os.Args) > 2 {
                
		fmt.Println("enter username:")
                fmt.Scanln(&args.Id)
		fmt.Println("enter passwd:")
		fmt.Scanln(&args.Passwd)
		// to register a client in server
		var rv bool
		err = client.Call("ServerTimer.Register", args, &rv)
		checkerror(err, "Calling ServerTimer.Register")
		if rv {
			fmt.Println("Register Successful!")
		} else {
			fmt.Println("Register Faliled,Reconnect server with your ID & password")
		}
	} else {

		fmt.Println("enter username:")
                fmt.Scanln(&args.Id)
		fmt.Println("enter passwd:")
		fmt.Scanln(&args.Passwd)
		
		mux := http.NewServeMux()
		staticDirHandler(mux, "/", "./clock")
		mux.HandleFunc("/test", testHandler)
		mux.HandleFunc("/time", timeHandler)
		mux.HandleFunc("/update", ajaxHandler(args, serveraddr, client))
		http.ListenAndServe(":8080", mux)
	}
}

func checkerror(err error, hint string) {
	if err != nil {
		fmt.Println(hint + ":" + err.Error())
		os.Exit(1)
	}
}
