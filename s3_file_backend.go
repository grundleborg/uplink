package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"sort"
	"time"

	"github.com/minio/minio-go"
	"github.com/spf13/viper"
)

type S3FileBackend struct {
	instanceId string

	entriesPerFile int
	sweepInterval  int64

	client *minio.Client

	payloadChannel chan *Payload

	schemaHeadersMap map[string]map[string][]string
	payloadStoreMap  map[string]map[string][]*Payload
}

func NewS3FileBackend() Backend {
	return S3FileBackend{
		instanceId:       viper.GetString(ConfigInstanceId),
		schemaHeadersMap: make(map[string]map[string][]string),
		payloadStoreMap:  make(map[string]map[string][]*Payload),
		payloadChannel:   make(chan *Payload),
		entriesPerFile:   viper.GetInt(ConfigEntriesPerFile),
		sweepInterval:    viper.GetInt64(ConfigSweepInterval),
	}
}

func (b S3FileBackend) Run() {
	var err error
	b.client, err = minio.New(
		viper.GetString(ConfigS3Endpoint),
		viper.GetString(ConfigS3AccessKeyId),
		viper.GetString(ConfigS3SecretAccessKey),
		viper.GetBool(ConfigS3UseSSL))
	checkError("cannot create minio client", err)

	exists, err := b.client.BucketExists(viper.GetString(ConfigS3BucketName))
	checkError("failed to check if bucket exists", err)

	if !exists {
		log.Fatalln("Bucket does not exist. Please create it before trying again.")
	}

	for {
		select {
		case payload := <-b.payloadChannel:
			b.updateHeadersFromPayload(payload)
			b.storePayload(payload)
			b.writeFileIfNecessary(payload.Warehouse, payload.Schema)
		}
	}
}

func (b S3FileBackend) GetPayloadChannel() chan<- *Payload {
	return b.payloadChannel
}

func (b S3FileBackend) GetHeaders(warehouse string, schema string) []string {
	_, okWarehouse := b.schemaHeadersMap[warehouse]
	if !okWarehouse {
		b.schemaHeadersMap[warehouse] = make(map[string][]string)
	}

	headers, okSchema := b.schemaHeadersMap[warehouse][schema]
	if okSchema {
		return headers
	}

	headers = []string{}
	b.schemaHeadersMap[warehouse][schema] = headers
	return headers
}

func (b S3FileBackend) ClearHeaders(warehouse string, schema string) {
	delete(b.schemaHeadersMap[warehouse], schema)
	if len(b.schemaHeadersMap[warehouse]) == 0 {
		delete(b.schemaHeadersMap, warehouse)
	}
}

func (b S3FileBackend) SetHeaders(warehouse string, schema string, headers []string) {
	b.schemaHeadersMap[warehouse][schema] = headers
}

func (b S3FileBackend) updateHeadersFromPayload(payload *Payload) {
	var keys []string
	for key := range payload.Data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	oldKeys := b.GetHeaders(payload.Warehouse, payload.Schema)

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

	b.SetHeaders(payload.Warehouse, payload.Schema, oldKeys)
}

func (b S3FileBackend) storePayload(payload *Payload) {
	_, okWarehouse := b.payloadStoreMap[payload.Warehouse]
	if !okWarehouse {
		b.payloadStoreMap[payload.Warehouse] = make(map[string][]*Payload)
	}

	payloads, okPayloads := b.payloadStoreMap[payload.Warehouse][payload.Schema]
	if !okPayloads {
		payloads = []*Payload{}
	}

	payloads = append(payloads, payload)
	b.payloadStoreMap[payload.Warehouse][payload.Schema] = payloads
}

func (b S3FileBackend) convertPayloadToStringList(payload *Payload) []string {
	keys := b.GetHeaders(payload.Warehouse, payload.Schema)

	var stringList []string
	stringList = append(stringList, payload.Id, payload.Source, fmt.Sprintf("%v", payload.ServerTimestamp), fmt.Sprintf("%v", payload.ClientTimestamp))
	for _, key := range keys {
		data, ok := payload.Data[key]
		if !ok {
			data = ""
		}
		stringList = append(stringList, fmt.Sprintf("%v", data))
	}
	return stringList
}

func (b S3FileBackend) writeFileIfNecessary(warehouse string, schema string) {
	payloads := b.payloadStoreMap[warehouse][schema]

	log.Printf("Payloads Length for Warehouse: %v and Schema: %v is: %v\n", warehouse, schema, len(payloads))

	if len(payloads) < b.entriesPerFile {
		return
	}

	fileName := fmt.Sprintf("%v-%v-%v-%v.csv", warehouse, schema, b.instanceId, time.Now().Unix())

	var buffer bytes.Buffer
	bufferWriter := bufio.NewWriter(&buffer)

	writer := csv.NewWriter(bufferWriter)
	writer.Comma = '|'

	headers := b.GetHeaders(warehouse, schema)
	headers = append([]string{"id", "source", "server_timestamp", "client_timestamp"}, headers...)
	writer.Write(headers)

	for _, payload := range payloads {
		stringList := b.convertPayloadToStringList(payload)
		writer.Write(stringList)
	}

	writer.Flush()

	err := bufferWriter.Flush()
	checkError("failed to flush buffer", err)

	_, err = b.client.PutObject(viper.GetString(ConfigS3BucketName), fileName, io.Reader(&buffer), int64(buffer.Len()), minio.PutObjectOptions{ContentType: "text/plain"})
	checkError("failed to put object to S3", err)

	b.ClearHeaders(warehouse, schema)
	delete(b.payloadStoreMap[warehouse], schema)
	if len(b.payloadStoreMap[warehouse]) == 0 {
		delete(b.payloadStoreMap, warehouse)
	}
}
