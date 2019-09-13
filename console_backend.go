package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"sort"
)

type ConsoleBackend struct {
	schemaWriterMap  map[string]*csv.Writer
	schemaHeadersMap map[string][]*string
	payloadChannel   chan *Payload
}

func NewConsoleBackend() Backend {
	return ConsoleBackend{
		schemaWriterMap:  make(map[string]*csv.Writer),
		schemaHeadersMap: make(map[string][]*string),
		payloadChannel:   make(chan *Payload),
	}
}

func (b ConsoleBackend) Run() {
	for {
		select {
		case payload := <-b.payloadChannel:
			w := b.getWriter(payload)
			w.Write(b.convertPayloadToStringList(payload))
			w.Flush()
		}
	}
}

func (b ConsoleBackend) GetPayloadChannel() chan<- *Payload {
	return b.payloadChannel
}

func (b ConsoleBackend) GetHeaders(schema string) []*string {
	headers, ok := b.schemaHeadersMap[schema]
	if ok {
		return headers
	}

	headers = []*string{}
	b.schemaHeadersMap[schema] = headers
	return headers
}

func (b ConsoleBackend) convertPayloadToStringList(payload *Payload) []string {
	var keys []string
	for key := range payload.Data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	oldKeys, ok := b.schemaHeadersMap[payload.Schema]
	if !ok {
		oldKeys = []*string{}
		b.schemaHeadersMap[payload.Schema] = oldKeys
	}

	for _, newKey := range keys {
		found := false
		for _, oldKey := range oldKeys {
			if *oldKey == newKey {
				found = true
				break
			}
		}
		if !found {
			oldKeys = append(oldKeys, &newKey)
		}
	}

	var stringList []string
	stringList = append(stringList, payload.Source)
	for _, key := range oldKeys {
		data, ok := payload.Data[*key]
		if !ok {
			data = ""
		}
		stringList = append(stringList, fmt.Sprintf("%v", data))
	}
	return stringList
}

func (b ConsoleBackend) getWriter(payload *Payload) *csv.Writer {
	writer, ok := b.schemaWriterMap[payload.Schema]
	if ok {
		return writer
	}

	writer = csv.NewWriter(os.Stdout)
	writer.Comma = '|'
	b.schemaWriterMap[payload.Schema] = writer
	return writer
}
