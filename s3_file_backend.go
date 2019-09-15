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

	schemaHeadersMap map[string][]string
	payloadStoreMap  map[string][]*Payload
}

func NewS3FileBackend() Backend {
	return S3FileBackend{
		instanceId:       viper.GetString(ConfigInstanceId),
		schemaHeadersMap: make(map[string][]string),
		payloadStoreMap:  make(map[string][]*Payload),
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
			b.writeFileIfNecessary(payload.Schema)
		}
	}
}

func (b S3FileBackend) GetPayloadChannel() chan<- *Payload {
	return b.payloadChannel
}

func (b S3FileBackend) GetHeaders(schema string) []string {
	headers, ok := b.schemaHeadersMap[schema]
	if ok {
		return headers
	}

	headers = []string{}
	b.schemaHeadersMap[schema] = headers
	return headers
}

func (b S3FileBackend) ClearHeaders(schema string) {
	delete(b.schemaHeadersMap, schema)
}

func (b S3FileBackend) SetHeaders(schema string, headers []string) {
	b.schemaHeadersMap[schema] = headers
}

func (b S3FileBackend) updateHeadersFromPayload(payload *Payload) {
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

func (b S3FileBackend) storePayload(payload *Payload) {
	payloads, ok := b.payloadStoreMap[payload.Schema]
	if !ok {
		payloads = []*Payload{}
		b.payloadStoreMap[payload.Schema] = payloads
	}
	payloads = append(payloads, payload)
	b.payloadStoreMap[payload.Schema] = payloads
}

func (b S3FileBackend) convertPayloadToStringList(payload *Payload) []string {
	keys := b.GetHeaders(payload.Schema)

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

func (b S3FileBackend) writeFileIfNecessary(schema string) {
	payloads := b.payloadStoreMap[schema]

	log.Printf("Payloads Length for Schema: %v is: %v\n", schema, len(payloads))

	if len(payloads) < b.entriesPerFile {
		return
	}

	fileName := fmt.Sprintf("%v-%v-%v.csv", schema, b.instanceId, time.Now().Unix())

	var buffer bytes.Buffer
	bufferWriter := bufio.NewWriter(&buffer)

	writer := csv.NewWriter(bufferWriter)
	writer.Comma = '|'

	headers := b.GetHeaders(schema)
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

	b.ClearHeaders(schema)
	delete(b.payloadStoreMap, schema)
}
