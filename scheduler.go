package main

import (
   "encoding/json"
   "github.com/joho/godotenv"
   "github.com/streadway/amqp"
   tamqp "github.com/trandoshan-io/amqp"
   "log"
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

   var crawledUrls = map[string]string{}

   // load .env
   if err := godotenv.Load(); err != nil {
      log.Fatal("Unable to load .env file: ", err.Error())
   }
   log.Println("Loaded .env file")

   // build list of forbidden extensions
   var forbiddenExtensions = []string{
      ".iso", ".xhtml", ".exe", ".css", ".img", ".png", ".jpg", ".jpeg", ".js", ".pdf", ".ico",
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
   if err := consumer.Consume(doneQueue, true, handleMessages(publisher, forbiddenExtensions, crawledUrls)); err != nil {
      log.Fatal("Unable to consume message: ", err.Error())
   }
   log.Println("Consumer initialized successfully")

   //TODO: better way
   select {}

   _ = consumer.Shutdown()
}

func handleMessages(publisher tamqp.Publisher, forbiddenExtensions []string, crawledUrls map[string]string) func(deliveries <-chan amqp.Delivery, done chan error) {
   return func(deliveries <-chan amqp.Delivery, done chan error) {
      for delivery := range deliveries {
         var url string

         // Unmarshal message
         if err := json.Unmarshal(delivery.Body, &url); err != nil {
            log.Println("Error while de-serializing payload: ", err.Error())
            break
         }

         if shouldParse(url, forbiddenExtensions, crawledUrls) {
            log.Println(url, " should be parsed")
            if err := publisher.PublishJson("", todoQueue, url); err != nil {
               log.Println("Error while trying to publish to done queue: ", err.Error())
            }
            crawledUrls[url] = ""
         }
      }
   }
}

// check if url contains not invalid stuff and if not already crawled
// todo plug memory cache to queue ?
func shouldParse(url string, forbiddenExtensions []string, crawledUrls map[string]string) bool {
   // make sure URL is not already managed
   if _, ok := crawledUrls[url]; ok {
      return false
   }

   // make sure URL does not contains forbidden extensions
   for _, forbiddenExtension := range forbiddenExtensions {
      if strings.HasSuffix(url, forbiddenExtension) {
         return false
      }
   }

   return true
}
