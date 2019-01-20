package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"

	"github.com/gorilla/mux"
)

type Payload struct {
	Source string                 `json:"source"`
	Schema string                 `json:"schema"`
	Data   map[string]interface{} `json:"data"`
}

var payloadChannel chan *Payload
var schemaWriterMap map[string]*csv.Writer
var schemaHeadersMap map[string]*[]string

func main() {
	payloadChannel = make(chan *Payload)
	schemaWriterMap = make(map[string]*csv.Writer)
	schemaHeadersMap = make(map[string]*[]string)

	go ProcessPayloads()

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

	payloadChannel <- &payload
}

func convertPayloadToStringList(payload *Payload) []string {
	var keys []string
	for key := range payload.Data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	oldKeys, ok := schemaHeadersMap[payload.Schema]
	if !ok {
		oldKeys = &[]string{}
	}

	for _, newKey := range keys {
		found := false
		for _, oldKey := range *oldKeys {
			if oldKey == newKey {
				found = true
				break
			}
		}
		if !found {
			*oldKeys = append(*oldKeys, newKey)
		}
	}

	var stringList []string
	stringList = append(stringList, payload.Source)
	for _, key := range *oldKeys {
		stringList = append(stringList, fmt.Sprintf("%v", payload.Data[key]))
	}
	return stringList
}

func getWriter(payload *Payload) *csv.Writer {
	writer, ok := schemaWriterMap[payload.Schema]
	if ok {
		return writer
	}

	writer = csv.NewWriter(os.Stdout)
	writer.Comma = '|'
	schemaWriterMap[payload.Schema] = writer
	return writer
}

func ProcessPayloads() {
	for {
		select {
		case payload := <-payloadChannel:
			w := getWriter(payload)
			w.Write(convertPayloadToStringList(payload))
			w.Flush()
		}
	}
}
