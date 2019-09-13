package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"github.com/spf13/viper"
)

type LocalFileBackend struct {
	schemaHeadersMap map[string][]string
	payloadStoreMap  map[string][]*Payload

	payloadChannel chan *Payload

	entriesPerFile int
	sweepInterval  int64
}

func NewLocalFileBackend() Backend {
	return LocalFileBackend{
		schemaHeadersMap: make(map[string][]string),
		payloadStoreMap:  make(map[string][]*Payload),
		payloadChannel:   make(chan *Payload),
		entriesPerFile:   viper.GetInt(ConfigEntriesPerFile),
		sweepInterval:    viper.GetInt64(ConfigSweepInterval),
	}
}

func (b LocalFileBackend) Run() {
	for {
		select {
		case payload := <-b.payloadChannel:
			b.updateHeadersFromPayload(payload)
			b.storePayload(payload)
			b.writeFileIfNecessary(payload.Schema)
		}
	}
}

func (b LocalFileBackend) GetPayloadChannel() chan<- *Payload {
	return b.payloadChannel
}

func (b LocalFileBackend) GetHeaders(schema string) []string {
	headers, ok := b.schemaHeadersMap[schema]
	if ok {
		return headers
	}

	headers = []string{}
	b.schemaHeadersMap[schema] = headers
	return headers
}

func (b LocalFileBackend) ClearHeaders(schema string) {
	delete(b.schemaHeadersMap, schema)
}

func (b LocalFileBackend) SetHeaders(schema string, headers []string) {
	b.schemaHeadersMap[schema] = headers
}

func (b LocalFileBackend) updateHeadersFromPayload(payload *Payload) {
	var keys []string
	for key := range payload.Data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	oldKeys := b.GetHeaders(payload.Schema)

	for _, newKey := range keys {
		found := false
		for _, oldKey := range oldKeys {
			if oldKey == newKey {
				found = true
				break
			}
		}
		if !found {
			oldKeys = append(oldKeys, newKey)
		}
	}

	b.SetHeaders(payload.Schema, oldKeys)
}

func (b LocalFileBackend) storePayload(payload *Payload) {
	payloads, ok := b.payloadStoreMap[payload.Schema]
	if !ok {
		payloads = []*Payload{}
		b.payloadStoreMap[payload.Schema] = payloads
	}
	payloads = append(payloads, payload)
	b.payloadStoreMap[payload.Schema] = payloads
}

func (b LocalFileBackend) convertPayloadToStringList(payload *Payload) []string {
	keys := b.GetHeaders(payload.Schema)

	var stringList []string
	stringList = append(stringList, payload.Source)
	for _, key := range keys {
		data, ok := payload.Data[key]
		if !ok {
			data = ""
		}
		stringList = append(stringList, fmt.Sprintf("%v", data))
	}
	return stringList
}

func (b LocalFileBackend) writeFileIfNecessary(schema string) {
	payloads := b.payloadStoreMap[schema]

	fmt.Printf("Payloads Length for Schema: %v is: %v\n", schema, len(payloads))

	if len(payloads) >= b.entriesPerFile {
		fileName := fmt.Sprintf("%v-%v.csv", schema, time.Now().Unix())

		file, err := os.Create(fileName)
		checkError("Cannot create file", err)
		defer file.Close()

		writer := csv.NewWriter(file)
		defer writer.Flush()
		writer.Comma = '|'

		headers := b.GetHeaders(schema)
		headers = append([]string{"source"}, headers...)
		writer.Write(headers)

		for _, payload := range payloads {
			stringList := b.convertPayloadToStringList(payload)
			writer.Write(stringList)
		}

		b.ClearHeaders(schema)
		delete(b.payloadStoreMap, schema)
	}
}

func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}
