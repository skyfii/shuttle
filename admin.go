package main

import (
	"encoding/json"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"github.com/skyfii/shuttle/client"
	"github.com/skyfii/shuttle/log"
	"github.com/gorilla/mux"
)

func getConfig(w http.ResponseWriter, r *http.Request) {
	w.Write(marshal(Registry.Config()))
}

func getStats(w http.ResponseWriter, r *http.Request) {
	if len(Registry.Config().Services) == 0 {
		w.WriteHeader(503)
	}
	w.Write(marshal(Registry.Stats()))
}

func getServiceStats(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	serviceStats, err := Registry.ServiceStats(vars["service"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Write(marshal(serviceStats))
}

func getServiceConfig(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	serviceStats, err := Registry.ServiceConfig(vars["service"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Write(marshal(serviceStats))
}

// Update the global config
func postConfig(w http.ResponseWriter, r *http.Request) {
	cfg := client.Config{}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorln("ERROR: ",err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	err = json.Unmarshal(body, &cfg)
	if err != nil {
		log.Errorln("ERROR: ",err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := Registry.UpdateConfig(cfg); err != nil {
		log.Errorln("ERROR: ",err)
		// TODO: differentiate between ServerError and BadRequest
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// Update a service and/or backends.
func postService(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorln("ERROR: ",err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	svcCfg := client.ServiceConfig{Name: vars["service"]}
	err = json.Unmarshal(body, &svcCfg)
	if err != nil {
		log.Errorln("ERROR: ",err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// don't let someone update the wrong service
	if svcCfg.Name != vars["service"] {
		errMsg := "Mismatched service name in API call"
		log.Errorln("ERROR: ",errMsg)
		http.Error(w, errMsg, http.StatusBadRequest)
		return
	}

	cfg := client.Config{
		Services: []client.ServiceConfig{svcCfg},
	}

	err = Registry.UpdateConfig(cfg)
	//FIXME: this doesn't return an error for an empty or broken service
	if err != nil {
		log.Error("ERROR: ",err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Write(marshal(Registry.Config()))
}

func deleteService(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	err := Registry.RemoveService(vars["service"])
	if err != nil {
		log.Errorf("ERROR: %s",err)
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	go writeStateConfig()
	w.Write(marshal(Registry.Config()))
}

func getBackendStats(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	serviceName := vars["service"]
	backendName := vars["backend"]

	backend, err := Registry.BackendStats(serviceName, backendName)
	if err != nil {
		log.Errorf("ERROR: %s",err)
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Write(marshal(backend))
}

func getBackend(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	serviceName := vars["service"]
	backendName := vars["backend"]

	backend, err := Registry.BackendStats(serviceName, backendName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Write(marshal(backend))
}

func postBackend(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorln("ERROR: ",err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	backendName := vars["backend"]
	serviceName := vars["service"]

	backendCfg := client.BackendConfig{Name: backendName}
	err = json.Unmarshal(body, &backendCfg)
	if err != nil {
		log.Errorln("ERROR: ",err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := Registry.AddBackend(serviceName, backendCfg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	go writeStateConfig()
	w.Write(marshal(Registry.Config()))
}

func deleteBackend(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	serviceName := vars["service"]
	backendName := vars["backend"]

	if err := Registry.RemoveBackend(serviceName, backendName); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	go writeStateConfig()
	w.Write(marshal(Registry.Config()))
}

func addHandlers() {
	r := mux.NewRouter()
	r.HandleFunc("/", getStats).Methods("GET")
	r.HandleFunc("/", postConfig).Methods("PUT", "POST")
	r.HandleFunc("/_config", getConfig).Methods("GET")
	r.HandleFunc("/_config", postConfig).Methods("PUT", "POST")
	r.HandleFunc("/_stats", getStats).Methods("GET")
	r.HandleFunc("/{service}", getServiceStats).Methods("GET")
	r.HandleFunc("/{service}/_config", getServiceConfig).Methods("GET")
	r.HandleFunc("/{service}/_stats", getServiceStats).Methods("GET")
	r.HandleFunc("/{service}", postService).Methods("PUT", "POST")
	r.HandleFunc("/{service}", deleteService).Methods("DELETE")
	r.HandleFunc("/{service}/{backend}", getBackend).Methods("GET")
	r.HandleFunc("/{service}/{backend}", postBackend).Methods("PUT", "POST")
	r.HandleFunc("/{service}/{backend}", deleteBackend).Methods("DELETE")
	http.Handle("/", r)
}

func startAdminHTTPServer(wg *sync.WaitGroup) {
	defer wg.Done()
	addHandlers()
	log.Println("INFO: Admin server listening on", adminListenAddr)

	netw := "tcp"

	if strings.HasPrefix(adminListenAddr, "/") {
		netw = "unix"

		// remove our old socket if we left it lying around
		if stats, err := os.Stat(adminListenAddr); err == nil {
			if stats.Mode()&os.ModeSocket != 0 {
				os.Remove(adminListenAddr)
			}
		}

		defer os.Remove(adminListenAddr)
	}

	listener, err := net.Listen(netw, adminListenAddr)
	if err != nil {
		log.Fatalf("FATAL: Admin server failed and exited with %s", err)
	}

	http.Serve(listener, nil)
}
