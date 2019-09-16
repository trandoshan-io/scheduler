package main

import (
   "encoding/json"
   "github.com/joho/godotenv"
   "github.com/streadway/amqp"
   tamqp "github.com/trandoshan-io/amqp"
   "log"
   "net/http"
   "os"
   "strconv"
   "strings"
)

const (
   todoQueue = "todo"
   doneQueue = "done"
)

//TODO: route message based on url ? like building a BTREE to dispatch message to crawler in a efficient manner
// this will allow high performance when dealing with a lot of urls (will reduce check complexity)
func main() {
   log.Println("Initializing scheduler")

   // load .env
   if err := godotenv.Load(); err != nil {
      log.Fatal("Unable to load .env file: ", err.Error())
   }
   log.Println("Loaded .env file")

   // load list of forbidden extensions
   response, err := http.Get(os.Getenv("API_URI") + "/forbidden-extensions")
   if err != nil {
      log.Fatal("Unable to load forbidden extensions from API: " + err.Error())
   }

   // un-marshal forbidden extensions
   var forbiddenExtensions []string
   if err = json.NewDecoder(response.Body).Decode(&forbiddenExtensions); err != nil {
      log.Fatal("Error while un-marshaling forbidden extensions: " + err.Error())
   }

   log.Println("Loaded " + strconv.Itoa(len(forbiddenExtensions)) + " forbidden extensions from the API")

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
   if err := consumer.Consume(doneQueue, false, handleMessages(publisher, forbiddenExtensions)); err != nil {
      log.Fatal("Unable to consume message: ", err.Error())
   }
   log.Println("Consumer initialized successfully")

   //TODO: better way
   select {}

   _ = consumer.Shutdown()
}

func handleMessages(publisher tamqp.Publisher, forbiddenExtensions []string) func(deliveries <-chan amqp.Delivery, done chan error) {
   return func(deliveries <-chan amqp.Delivery, done chan error) {
      for delivery := range deliveries {
         var url string

         // Unmarshal message
         if err := json.Unmarshal(delivery.Body, &url); err != nil {
            log.Println("Error while de-serializing payload: ", err.Error())
            _ = delivery.Reject(false)
            continue
         }

         // clean / sanitize url
         cleanUrl := strings.TrimSuffix(url, "/")

         // make sure url is not crawled
         if shouldParse(cleanUrl, forbiddenExtensions) {
            log.Println(url, " should be parsed")
            if err := publisher.PublishJson("", todoQueue, url); err != nil {
               log.Println("Error while trying to publish to done queue: ", err.Error())
               _ = delivery.Reject(false)
            }
         }

         _ = delivery.Ack(false)
      }
   }
}

// check if url contains not invalid stuff and if not already crawled
func shouldParse(url string, forbiddenExtensions []string) bool {
   // make sure URL is a valid .onion URL
   //TODO: improve this check
   if !strings.Contains(url, ".onion") {
      return false
   }

   // make sure URL does not contains forbidden extensions
   //TODO: remove and let crawler do the check?
   for _, forbiddenExtension := range forbiddenExtensions {
      if strings.HasSuffix(url, forbiddenExtension) {
         return false
      }
   }

   // make sure URL is not already managed
   // this is done in the last part because heaviest operation
   resp, err := http.Get(os.Getenv("API_URI") + "/pages?url=" + url)
   if err != nil {
      log.Println("Error while checking if url " + url + " has been crawled. Assuming not")
      return true
   }

   var body []map[string]interface{}
   if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
      log.Println("Error while decoding json body. Assuming " + url + " has not been crawled: " + err.Error())
      return true
   }

   // result: url has been crawled
   if len(body) > 0 {
      return false
   }

   return true
}