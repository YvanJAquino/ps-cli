package main

import (
   "context"
   "encoding/json"
   "flag"
   "fmt"
   "os"

   "cloud.google.com/go/pubsub"
)

type Config struct {
   ProjectID  string
   Topic     string
}

func handleError(err error, desc string) {
   if err != nil {
      fmt.Println("Error during: ", desc)
      fmt.Println("Error description:" , err)
      os.Exit(1)
   }
}

func readFile(path *string) *[]byte {
   data, err := os.ReadFile(*path)
   handleError(err, "os.ReadFile")
   return &data
}

func unmarshal(data []byte)  *Config{
   var cfg Config
   err := json.Unmarshal(data, &cfg)
   handleError(err, "json.Unmarshal")
   return &cfg
}

func readConfig(path *string) Config {
   data := readFile(path)
   cfg := unmarshal(*data)
   return *cfg
}

func makePubsubClient(ctx context.Context, projectID string) pubsub.Client {
   client, err := pubsub.NewClient(ctx, projectID)
   handleError(err, "pubsub.NewClient")
   return *client
}

func stageTopic(cfg Config) (context.Context, *pubsub.Topic) {
   ctx := context.Background()
   client := makePubsubClient(ctx, cfg.ProjectID)
   topic := client.Topic(cfg.Topic)
   return ctx, topic
}

func prepareMessage(msg *string) pubsub.Message {
   message := pubsub.Message{Data: []byte(*msg)}
   return message
}

func publishMessage(ctx context.Context, message *pubsub.Message, topic *pubsub.Topic) *pubsub.PublishResult {
   result := topic.Publish(ctx, message)
   return result
}

func printPublishResult(ctx context.Context, result *pubsub.PublishResult) {
   id, err := result.Get(ctx)
   handleError(err, "result.Get")
   fmt.Println("Published message id: ", id)
}

var argMsg = flag.String("m", "Default message", "Message for Cloud PubSub.  ")

func main() {
   path := "cfg.json"
   cfg := readConfig(&path)
   ctx, topic := stageTopic(cfg)
   defer topic.Stop()
   flag.Parse()
   msg := prepareMessage(argMsg)
   result := publishMessage(ctx, &msg, topic)
   printPublishResult(ctx, result)
}