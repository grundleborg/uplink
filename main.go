package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

type Payload struct {
	Source string                 `json:"source"`
	Schema string                 `json:"schema"`
	Data   map[string]interface{} `json:"data"`
}

var backend Backend

func main() {
	// Initialise global state.
	//backend = NewConsoleBackend()
	backend = NewLocalFileBackend(100, 3600)

	// Start background goroutines.
	go backend.Run()

	// Start web server.
	router := mux.NewRouter()
	router.HandleFunc("/log", ReceivePayload).Methods("POST")
	log.Fatal(http.ListenAndServe(":8000", router))
}

func ReceivePayload(w http.ResponseWriter, r *http.Request) {
	var payload Payload

	err := json.NewDecoder(r.Body).Decode(&payload)
	if err != nil {
		log.Printf("Failed to decode body: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	channel := backend.GetPayloadChannel()
	channel <- &payload
}

type Backend interface {
	Run()
	GetPayloadChannel()  chan<- *Payload
}
