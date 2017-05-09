package server

import (
	"fmt"
	"net/http"
	"os"

	"context"
	//"log"
	"oakleaf/config"
	"path/filepath"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/kabukky/httpscerts"
)

//var FileList

var Stop = make(chan bool, 1)
var Stopped = make(chan bool, 1)

var ShuttingDown = false

func Start(port int) {
	go nodeServerWorker(conf, port)
}

func nodeServerWorker(c *config.Config, port int) {
	log.Info("API-server is starting...")
	r := mux.NewRouter() //.StrictSlash(true)
	//r.HandleFunc("/", HomeHandler)
	r.HandleFunc("/api/parts", partUploadHandler).Methods("POST")
	//r.HandleFunc("/parts", partsListHandler)
	//staticPartHandler := http.StripPrefix("/parts/", http.FileServer(http.Dir(NodeConfig.DataDir)))
	///r.PathPrefix("/parts").Handler(http.StripPrefix("/", http.FileServer(http.Dir(NodeConfig.DataDir)))).Methods("GET")
	//http.Handle("/", r)
	//r.HandleFunc("/parts/", staticPartHandler).Methods("GET")
	//r.HandleFunc("/parts/{id}", partDownloadHandler).Methods("GET")
	r.HandleFunc("/cluster", nodeListHandler).Methods("GET")
	r.HandleFunc("/api/check/part/{id}", partCheckExistanceHandler).Methods("GET")
	r.PathPrefix("/api/part/").Handler(http.StripPrefix("/api/part/",
		http.FileServer(http.Dir(c.DataDir)))).Methods("GET")
	r.HandleFunc("/api/part/{id}", partDeleteHandler).Methods("DELETE")
	r.HandleFunc("/api/files", fileListHandler).Methods("GET")
	r.HandleFunc("/api/files", fileUploadHandler).Methods("POST")
	r.HandleFunc("/file/{id}", fileDownloadHandler).Methods("GET")
	r.HandleFunc("/api/file/info", getFileInfoHandler).Methods("POST")
	r.HandleFunc("/api/part/info", changePartInfoHandler).Methods("POST")
	r.HandleFunc("/api/file/info/{id}", fileInfoHandler).Methods("GET")
	r.HandleFunc("/api/file/{id}", fileDeleteHandler).Methods("DELETE")
	r.HandleFunc("/api/cluster", nodeListHandlerAPI)
	r.HandleFunc("/api/node/exit", nodeExitClusterHanderAPI)
	r.HandleFunc("/api/node/info", nodeInfoHandler)
	r.HandleFunc("/api/cluster/rebalance", rebalanceHandler).Methods("GET", "POST")
	r.PathPrefix("/").Handler(http.StripPrefix("/",
		http.FileServer(http.Dir(os.Getenv("GOPATH")+"/src/github.com/darkworon/oakleaf/web")))).Methods("GET")

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
	certPath := filepath.Join(c.WorkingDir, "cert.pem")
	keyPath := filepath.Join(c.WorkingDir, "key.pem")
	if conf.UseTLS {
		err := httpscerts.Check(certPath, keyPath)
		// If they are not available, generate new ones.
		if err != nil {
			err = httpscerts.Generate(certPath, keyPath, "localhost,127.0.0.1,::1")
			if err != nil {
				log.Error("Error: Couldn't create https certs.")
			}
		}
	}

	srv := &http.Server{
		Handler: r,
		Addr:    ":" + strconv.Itoa(port),
		// Good practice: enforce timeouts for servers you create!
		WriteTimeout: 20 * time.Minute,
		ReadTimeout:  20 * time.Minute,
	}
	srv.SetKeepAlivesEnabled(false)
	go func() {
		//fmt.Println(srv.Addr)
		var err error
		if conf.UseTLS {
			err = srv.ListenAndServeTLS(certPath, keyPath)
		} else {
			err = srv.ListenAndServe()
		}
		if err != nil {
			log.Error(err)
		}
	}()
	<-Stop
	log.Info("Shutting down server...")
	jobsDone := make(chan bool, 1)
	go func() {
		defer close(jobsDone)
		for JobsCount() > 0 {
			//fmt.Println("Jobs count:", JobsCount())
			time.Sleep(1 * time.Second)
		}
		jobsDone <- true
	}()
	select {
	case <-jobsDone:
		// okay
	case <-time.After(5 * time.Minute):
		// timeout
		log.Error("Timeout reached. Shutting down forcely...")
	}
	shuttedDown := make(chan bool, 1)
	go func() {
		defer close(shuttedDown)
		srv.Shutdown(context.Background())
		shuttedDown <- true
	}()
	select {
	case <-shuttedDown:
		// okay
	case <-time.After(5 * time.Minute):
		// timeout
		log.Error("Timeout reached. Closing tcp port...")
		srv.Close()
	}
	Stopped <- true
	close(Stopped)

}
