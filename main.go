package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"time"

	"github.com/gorilla/mux"
	"github.com/pborman/uuid"
	"github.com/spf13/viper"
)

const (
	BackendConsole   = "console"
	BackendLocalFile = "localfile"
	BackendS3File    = "s3file"

	ConfigEnvVarPrefix = "UPLINK"

	ConfigInstanceId     = "InstanceId"
	ConfigEntriesPerFile = "EntriesPerFile"
	ConfigSweepInterval  = "SweepInterval"
	ConfigBackend        = "Backend"

	ConfigS3Endpoint        = "S3Endpoint"
	ConfigS3AccessKeyId     = "S3AccessKeyId"
	ConfigS3SecretAccessKey = "S3SecretAccessKey"
	ConfigS3UseSSL          = "S3UseSSL"
	ConfigS3BucketName      = "S3BucketName"
	ConfigS3Location        = "S3Location"
)

type Payload struct {
	Id              string                 `json:"id"`
	Warehouse       string                 `json:"warehouse"`
	Source          string                 `json:"source"`
	Schema          string                 `json:"schema"`
	ClientTimestamp int64                  `json:"client_timestamp"`
	ServerTimestamp int64                  `json:"server_timestamp"`
	Data            map[string]interface{} `json:"data"`
}

var backend Backend

func setupConfig() {
	viper.SetDefault(ConfigInstanceId, NewInstanceId())
	viper.SetDefault(ConfigEntriesPerFile, 1000)
	viper.SetDefault(ConfigSweepInterval, 60)
	viper.SetDefault(ConfigBackend, BackendConsole)

	viper.SetDefault(ConfigS3Endpoint, "localhost:9000")
	viper.SetDefault(ConfigS3AccessKeyId, "")
	viper.SetDefault(ConfigS3SecretAccessKey, "")
	viper.SetDefault(ConfigS3UseSSL, false)
	viper.SetDefault(ConfigS3BucketName, "uplink")
	viper.SetDefault(ConfigS3Location, "us-east-1")

	viper.SetEnvPrefix(ConfigEnvVarPrefix)
	viper.AutomaticEnv()
}

func setupBackend() Backend {
	switch viper.GetString(ConfigBackend) {
	case BackendConsole:
		return NewConsoleBackend()
	case BackendLocalFile:
		return NewLocalFileBackend()
	case BackendS3File:
		return NewS3FileBackend()
	default:
		return nil
	}
}

func main() {
	setupConfig()

	log.Printf("Launching Uplink Server with instance ID: %v\n", viper.GetString(ConfigInstanceId))
	log.Printf("Using Backend: %v\n", viper.GetString(ConfigBackend))

	backend = setupBackend()

	// Start background goroutines.
	go backend.Run()

	// Start web server.
	router := mux.NewRouter()
	router.HandleFunc("/v0/log", ReceivePayload).Methods("POST")
	router.HandleFunc("/v0/log", PreflightResponder).Methods("OPTIONS")
	log.Fatal(http.ListenAndServe(":8000", router))
}

func PreflightResponder(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
}

func ReceivePayload(w http.ResponseWriter, r *http.Request) {
	var payload Payload

	err := json.NewDecoder(r.Body).Decode(&payload)
	if err != nil {
		log.Printf("Failed to decode body: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	payload.ServerTimestamp = GetMillis()
	payload.Id = uuid.NewRandom().String()

	if validationResult := ValidatePayload(&payload); validationResult != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(*validationResult))
		return
	}

	channel := backend.GetPayloadChannel()
	channel <- &payload
}

func newString(s string) *string {
	return &s
}

const (
	warehouseRegex = "^[a-z][0-9a-z]+$"
	schemaRegex = "^[a-z][0-9a-z]+$"
)

var validWarehouse = regexp.MustCompile(warehouseRegex)
var validSchema = regexp.MustCompile(schemaRegex)

func ValidatePayload(payload *Payload) *string {
	if payload.ClientTimestamp <= 0 {
		return newString("client_timestamp field must be greater than 0")
	}

	if !validWarehouse.MatchString(payload.Warehouse) {
		return newString(fmt.Sprintf("Warehouse \"%v\" contains unacceptable characters. Warehouse names must match the following regular expression: %v", payload.Warehouse, warehouseRegex))
	}

	if !validSchema.MatchString(payload.Schema) {
		return newString(fmt.Sprintf("Schema \"%v\" contains unacceptable characters. Schema names must match the following regular expression: %v", payload.Schema, schemaRegex))
	}

	if payload.Data == nil {
		return newString("At least one data field must be provided in the payload")
	}

	for key, _ := range payload.Data {
		if keyMsg := ValidateKey(key); keyMsg != nil {
			return keyMsg
		}
	}

	return nil
}

const keyRegexp = "^[a-z][0-9a-z_]*[a-z0-9]$"
var validKey = regexp.MustCompile(keyRegexp)
var forbiddenKeys = []string{"id", "server_timestamp", "client_timestamp", "source", "event"}

func ValidateKey(key string) *string {
	if !validKey.MatchString(key) {
		return newString(fmt.Sprintf("Data key \"%v\" contains unacceptable characters. Key names must match the following regular expression: %v", key, keyRegexp))
	}

	for _, value := range forbiddenKeys {
		if value == key {
			return newString(fmt.Sprintf("Data key \"%v\" is a reserved word and must not be used", key))
		}
	}

	if len(key) < 2 && len(key) > 128 {
		return newString(fmt.Sprintf("Data key \"%v\" is too long. It must be less than 128 characters"))
	}

	return nil
}

type Backend interface {
	Run()
	GetPayloadChannel() chan<- *Payload
}

var encoding = base32.NewEncoding("ybndrfg8ejkmcpqxot1uwisza345h769")

func NewInstanceId() string {
	randomBytes := make([]byte, 5)
	_, err := rand.Read(randomBytes)
	checkError("failed to generate random ID", err)

	var b bytes.Buffer
	encoder := base32.NewEncoder(encoding, &b)
	encoder.Write(randomBytes)
	encoder.Close()
	return b.String()
}

func GetMillis() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
