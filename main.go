package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
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
	Source string                 `json:"source"`
	Schema string                 `json:"schema"`
	Data   map[string]interface{} `json:"data"`
}

var backend Backend

type AppConfig struct {
	instanceId string
}

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
	log.Println("Launching Uplink Server...")

	setupConfig()

	log.Printf("Using Backend: %v\n", viper.GetString(ConfigBackend))

	backend = setupBackend()

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
	GetPayloadChannel() chan<- *Payload
}

func GenerateRandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	// Note that err == nil only if we read len(b) bytes.
	if err != nil {
		return nil, err
	}

	return b, nil
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
