package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/nats-io/nats.go"
	"log"
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

	// Connect to elasticsearch
	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		log.Fatalf("Error creating elasticsearch client: %s", err)
	}
	log.Printf("Elasticsearch client successfully created")

	// initialize queue subscriber
	if _, err := nc.QueueSubscribe(doneSubject, crawlingQueue, handleMessages(nc, es)); err != nil {
		log.Fatalf("Error while trying to subscribe to server: %s", err)
	}

	log.Print("Consumer initialized successfully")

	//TODO: better way
	select {}
}

func handleMessages(nc *nats.Conn, es *elasticsearch.Client) func(*nats.Msg) {
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
		if shouldCrawl(es, cleanUrl) {
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

// check if resource should be scheduled for crawling
func shouldCrawl(es *elasticsearch.Client, url string) bool {
	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"url": url,
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		log.Printf("Error encoding query: %s", err)
		return false
	}

	// Perform the search request.
	res, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex("resources"),
		es.Search.WithBody(&buf),
		es.Search.WithTrackTotalHits(true),
		es.Search.WithPretty(),
	)
	if err != nil {
		log.Printf("Error getting response: %s", err)
		return false
	}
	if res.IsError() {
		log.Printf("Error getting response: %s", err)
		return false
	}

	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}

	return len(r["hits"].(map[string]interface{})["hits"].([]interface{})) == 0
}
