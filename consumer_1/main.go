package main

import (
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
)

const MIN_MESSAGE_COUNT = 100

var kafkaServer string
var kafkaTopic string
var groupId string
var consumer *kafka.Consumer

func processMessage(msg *kafka.Message) {
	fmt.Printf("received message: %s\n", string(msg.Value))
}

func main() {

	err := godotenv.Load()

	if err != nil {
		panic(err)

	}

	kafkaServer = os.Getenv("KAFKA_SERVER")
	kafkaTopic = os.Getenv("KAFKA_TOPIC")
	groupId = os.Getenv("GROUP_ID")

	consumer, err = kafka.NewConsumer(
		&kafka.ConfigMap{
			"bootstrap.servers": kafkaServer,
			"group.id":          groupId,
			"auto.offset.reset": "smallest",
		})

	if err != nil {
		log.Fatalf("Failed to create a consumer. %s", err)

	}
	defer consumer.Close()

	topics := []string{kafkaTopic}

	err = consumer.SubscribeTopics(topics, nil)

	if err != nil {
		log.Fatalf("Falied to subscribe to the topic %s. %e", kafkaTopic, err)
	}

	fmt.Println("start processing messgae.")

	msgCount := 0
	for {
		ev := consumer.Poll(100)
		switch et := ev.(type) {
		case *kafka.Message:
			msgCount += 1
			processMessage(et)
			if msgCount&MIN_MESSAGE_COUNT == 0 {
				_, err := consumer.Commit()
				if err != nil {
					log.Printf("failed to process message. %s", et.Value)
				}

			} else {
				log.Printf("committed offsets.")
			}

			fmt.Println("hello")
		case kafka.Error:
			fmt.Println("something went wrong. %s", et.String())

		}
	}

}
