package main

import (
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"log"
	"net/http"
	"os"
	"strings"
)

const (
	crawlingQueue = "crawlingQueue"
	todoSubject   = "todoSubject"
	doneSubject   = "doneSubject"
)

func main() {
	log.Print("Initializing scheduler")

	// connect to NATS server
	nc, err := nats.Connect(os.Getenv("NATS_URI"))
	if err != nil {
		log.Fatalf("Error while connecting to nats server: %s", err)
	}
	defer nc.Close()

	// initialize queue subscriber
	if _, err := nc.QueueSubscribe(doneSubject, crawlingQueue, handleMessages(nc)); err != nil {
		log.Fatalf("Error while trying to subscribe to server: %s", err)
	}

	log.Print("Consumer initialized successfully")

	//TODO: better way
	select {}
}

func handleMessages(nc *nats.Conn) func(*nats.Msg) {
	return func(msg *nats.Msg) {
		var url string

		// Unmarshal message
		if err := json.Unmarshal(msg.Data, &url); err != nil {
			log.Printf("Error while de-serializing payload: %s", err)
			// todo: store in sort of DLQ?
			return
		}

		// clean / sanitize url
		cleanUrl := strings.TrimSuffix(url, "/")

		// make sure url is not crawled
		if shouldParse(cleanUrl) {
			log.Printf("%s should be parsed", url)

			// publish url in todo queue
			bytes, err := json.Marshal(url)
			if err != nil {
				log.Printf("Error while serializing message into json: %s", err)
				return
			}
			if err = nc.Publish(todoSubject, bytes); err != nil {
				log.Printf("Error while trying to publish to done queue: %s", err)
				// todo: store in sort of DLQ?
			}
		}
	}
}

// check if url contains not invalid stuff and if not already crawled
func shouldParse(url string) bool {
	// make sure URL is not already managed
	resp, err := http.Get(fmt.Sprintf("%s/pages?url=%s", os.Getenv("API_URI"), url))
	if err != nil {
		log.Printf("Error while checking if url %s has been crawled. Assuming not", url)
		return true
	}

	var body []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		log.Printf("Error while decoding json body. Assuming %s has not been crawled: %s", url, err)
		return true
	}

	// result: url has been crawled
	if len(body) > 0 {
		return false
	}

	return true
}
