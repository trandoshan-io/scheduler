package main

import (
	"encoding/json"
	"github.com/joho/godotenv"
	"github.com/streadway/amqp"
	tamqp "github.com/trandoshan-io/amqp"
	"log"
	"os"
	"strconv"
	"time"
)

const (
	todoQueue = "todo"
	doneQueue = "done"
)

func main() {
	log.Println("Initializing crawler")

	// load .env
	if err := godotenv.Load(); err != nil {
		log.Fatal("Unable to load .env file: ", err.Error())
	}
	log.Println("Loaded .env file")

	// allow some kind of boot delay to fix usage in docker-compose
	// TODO: find a better way
	if startupDelay := os.Getenv("STARTUP_DELAY"); startupDelay != "" {
		val, _ := strconv.Atoi(startupDelay)
		time.Sleep(time.Duration(val) * time.Second)
	}

	prefetch, err := strconv.Atoi(os.Getenv("AMQP_PREFETCH"))
	if err != nil {
		log.Fatal(err)
	}

	// initialize publishers
	publisher, err := tamqp.NewStateFullPublisher(os.Getenv("AMQP_URI"))
	if err != nil {
		log.Fatal("Unable  to create publisher: ", err.Error())
	}
	log.Println("Publisher initialized successfully")

	// initialize consumer & start him
	consumer, err := tamqp.NewConsumer(os.Getenv("AMQP_URI"), prefetch)
	if err != nil {
		log.Fatal("Unable to create consumer: ", err.Error())
	}
	log.Println("Consumer initialized successfully")

	if err := consumer.Consume(doneQueue, true, handleMessages(publisher)); err != nil {
		log.Fatal("Unable to consume message: ", err.Error())
	}

	//TODO: better way
	select {}

	_ = consumer.Shutdown()
}

func handleMessages(publisher tamqp.Publisher) func(deliveries <-chan amqp.Delivery, done chan error) {
	return func(deliveries <-chan amqp.Delivery, done chan error) {
		for delivery := range deliveries {
			var url string

			// Unmarshal message
			if err := json.Unmarshal(delivery.Body, &url); err != nil {
				log.Println("Error while deserializing payload: ", err.Error())
				break
			}

			if shouldParse(url) {
				log.Println(url, " should be parsed")
				if err := publisher.PublishJson("", todoQueue, url); err != nil {
					log.Println("Error while trying to publish to done queue: ", err.Error())
				}
			}
		}
	}
}

// check if url contains not invalid stuff and if not already crawled
func shouldParse(url string) bool {
	return true // todo
}
