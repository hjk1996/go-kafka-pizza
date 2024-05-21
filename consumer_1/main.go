package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

const MIN_MESSAGE_COUNT = 100

var kafkaServer string
var kafkaConsumerTopic string
var kafkaProducerTopic string
var groupId string
var consumer *kafka.Consumer
var producer *kafka.Producer

type PizzaMenu struct {
	Name     string `json:"name"`
	Price    int    `json:"price"`
	Quantity int    `json:"Quantity"`
}

type Order struct {
	Id        string      `json:"id"`
	Orderer   string      `json:"orderer"`
	Menus     []PizzaMenu `json:"menus"`
	CreatedAt time.Time   `json:"createdAt"`
}

type PizzaUncooked struct {
	Id        string    `json:"id"`
	Orderer   string    `json:"orderer"`
	MenuName  string    `json:"menuName"`
	CreatedAt time.Time `json:createdAt`
}

func processMessage(msg *kafka.Message) {
	var order Order
	fmt.Printf("received message: %s\n", string(msg.Value))

	err := json.Unmarshal(msg.Value, &order)

	if err != nil {
		fmt.Println("failed to process the order: %s", order)
		return
	}

	for _, menu := range order.Menus {
		for i := 0; i < menu.Quantity; i++ {
			go func() {
				newUUID := uuid.New().String()
				pizza := PizzaUncooked{
					Id:        newUUID,
					Orderer:   order.Orderer,
					MenuName:  menu.Name,
					CreatedAt: time.Now(),
				}

				msg, err := json.Marshal(pizza)

				if err != nil {
					fmt.Println("failed to process the menu")
				}

				err = producer.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{
						Topic:     &kafkaProducerTopic,
						Partition: kafka.PartitionAny,
					},
					Value: msg,
				}, nil,
				)

				if err != nil {
					fmt.Println("failed to place the menu to the kitchen")
				}

			}()

		}

	}

}

func main() {

	err := godotenv.Load()

	if err != nil {
		panic(err)

	}

	kafkaServer = os.Getenv("KAFKA_SERVER")
	kafkaConsumerTopic = os.Getenv("KAFKA_CONSUMER_TOPIC")
	kafkaProducerTopic = os.Getenv("KAFKA_PRODUCER_TOPIC")
	groupId = os.Getenv("GROUP_ID")

	fmt.Println("Creating a consumer...")
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

	fmt.Println("Creating a producer")
	producer, err = kafka.NewProducer(
		&kafka.ConfigMap{
			"bootstrap.servers": kafkaServer,
		},
	)

	if err != nil {
		log.Fatalf("failed to create a producer. %s", err)
	}
	defer producer.Close()

	topics := []string{kafkaConsumerTopic}

	err = consumer.SubscribeTopics(topics, nil)

	if err != nil {
		log.Fatalf("Falied to subscribe to the topic %s. %e", kafkaConsumerTopic, err)
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
