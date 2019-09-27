package main

import (
   "encoding/json"
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
   log.Println("Initializing scheduler")

   // connect to NATS server
   nc, err := nats.Connect(os.Getenv("NATS_URI"))
   if err != nil {
      log.Fatal("Error while connecting to nats server: ", err)
   }
   defer nc.Close()

   // initialize queue subscriber
   if _, err := nc.QueueSubscribe(doneSubject, crawlingQueue, handleMessages(nc)); err != nil {
      log.Fatal("Error while trying to subscribe to server: ", err)
   }

   log.Println("Consumer initialized successfully")

   //TODO: better way
   select {}
}

func handleMessages(nc *nats.Conn) func(*nats.Msg) {
   return func(msg *nats.Msg) {
      var url string

      // Unmarshal message
      if err := json.Unmarshal(msg.Data, &url); err != nil {
         log.Println("Error while de-serializing payload: ", err)
         // todo: store in sort of DLQ?
         return
      }

      // clean / sanitize url
      cleanUrl := strings.TrimSuffix(url, "/")

      // make sure url is not crawled
      if shouldParse(cleanUrl) {
         log.Println(url, " should be parsed")

         // publish url in todo queue
         bytes, err := json.Marshal(url)
         if err != nil {
            log.Println("Error while serializing message into json: ", err)
            return
         }
         if err = nc.Publish(todoSubject, bytes); err != nil {
            log.Println("Error while trying to publish to done queue: ", err)
            // todo: store in sort of DLQ?
         }
      }
   }
}

// check if url contains not invalid stuff and if not already crawled
func shouldParse(url string) bool {
   // make sure URL is not already managed
   // this is done in the last part because heaviest operation
   resp, err := http.Get(os.Getenv("API_URI") + "/pages?url=" + url)
   if err != nil {
      log.Println("Error while checking if url " + url + " has been crawled. Assuming not")
      return true
   }

   var body []map[string]interface{}
   if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
      log.Println("Error while decoding json body. Assuming " + url + " has not been crawled: ", err)
      return true
   }

   // result: url has been crawled
   if len(body) > 0 {
      return false
   }

   return true
}