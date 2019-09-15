package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	//"github.com/pborman/uuid"
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

	payload.ServerTimestamp = GetMillis()
	//payload.Id = uuid.NewRandom()
	payload.Id = "12345"

	channel := backend.GetPayloadChannel()
	channel <- &payload
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
