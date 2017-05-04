package server

import (
	//"bufio"
	//"bytes"
	//"encoding/base64"
	//"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
	//"runtime"
	//"runtime/pprof"
	//"log"
	//"runtime/pprof"
	//"oakleaf/cluster"
	//"oakleaf/config"
	//"oakleaf/file"
	//"oakleaf/cluster"
	"oakleaf/config"
	"oakleaf/utils"
	"strconv"
	"time"
	//"oakleaf/file"
)

//var FileList

func Start(port int) {
	go nodeServerWorker(conf, port)
}

func nodeServerWorker(c *config.Config, port int) {
	fmt.Println("[INFO] Master http-server is starting...")
	r := mux.NewRouter() //.StrictSlash(true)
	//r.HandleFunc("/", HomeHandler)
	r.HandleFunc("/part", partUploadHandler).Methods("POST")
	//r.HandleFunc("/parts", partsListHandler)
	//staticPartHandler := http.StripPrefix("/part/", http.FileServer(http.Dir(NodeConfig.DataDir)))
	///r.PathPrefix("/part").Handler(http.StripPrefix("/", http.FileServer(http.Dir(NodeConfig.DataDir)))).Methods("GET")
	//http.Handle("/", r)
	//r.HandleFunc("/part/", staticPartHandler).Methods("GET")
	//r.HandleFunc("/part/{id}", partDownloadHandler).Methods("GET")
	r.PathPrefix("/part/").Handler(http.StripPrefix("/part/",
		http.FileServer(http.Dir(c.DataDir)))).Methods("GET")
	r.HandleFunc("/part/{id}", partDeleteHandler).Methods("DELETE")
	r.HandleFunc("/file", fileUploadHandler).Methods("POST")
	r.HandleFunc("/files", fileListHandler)
	r.HandleFunc("/file/{id}", fileDownloadHandler).Methods("GET")
	r.HandleFunc("/file/info", getFileInfoHandler).Methods("POST")
	r.HandleFunc("/file/info/{id}", fileInfoHandler).Methods("GET")
	r.HandleFunc("/file/{id}", fileDeleteHandler).Methods("DELETE")
	r.HandleFunc("/nodes", nodeListHandler)
	r.HandleFunc("/node/info", nodeInfoHandler)

	//r.HandleFunc("/articles", ArticlesHandler)
	//http.HandleFunc("/", fileDownloadHandler)
	//http.HandleFunc("/", fileUploadHandler)
	//http.ListenAndServe(":8086", nil)
	fmt.Printf("###########################\nAvailable methods on API:\n\n")
	r.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		t, err := route.GetPathTemplate()
		if err != nil {
			return err
		}
		fmt.Println("> " + t)
		return nil
	})
	fmt.Printf("###########################\n")

	srv := &http.Server{
		Handler: r,
		Addr:    ":" + strconv.Itoa(port),
		// Good practice: enforce timeouts for servers you create!
		WriteTimeout: 5 * time.Minute,
		ReadTimeout:  5 * time.Minute,
	}
	//fmt.Println(srv.Addr)
	err := srv.ListenAndServe()
	if err != nil {
		utils.HandleError(err)
	}

}
