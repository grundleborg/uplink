package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"fmt"
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

type AppConfig struct {
	instanceId string
}

func main() {
	fmt.Println("Launching Uplink Server...")
	// Initialise global state.
	//backend = NewConsoleBackend()
	//backend = NewLocalFileBackend(100, 3600)
	backend = NewS3FileBackend(
		100,
		3600,
		S3Config{
			endpoint:        "127.0.0.1:9000",
			accessKeyId:     "9087R1WSRBOF7TWA0KFO",
			secretAccessKey: "JFbZaA6qTB3kdcdXln9X78Qqcby/jLa3xaAhNj8g",
			useSSL:          false,
			bucketName:      "uplink",
			location:        "us-east-1",
		},
		AppConfig{
			instanceId: NewInstanceId(),
		})

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
