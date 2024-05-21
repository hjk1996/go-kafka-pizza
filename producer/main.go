package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
)

var prod *kafka.Producer
var kafkaServer string
var kafkaTopic string

type Order struct {
	Id          string    `json:"id"`
	ProductName string    `json:"productName"`
	Quantity    int       `json:"quantity"`
	Price       int       `json:"price"`
	CreatedAt   time.Time `json:"createdAt"`
}

func createTopic(server string, topic string) {

	fmt.Printf("Creating topic: server=%s, topic=%s\n", server, topic) // 디버깅 출력 추가

	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": server})
	if err != nil {
		panic(err)
	}
	defer adminClient.Close()

	results, err := adminClient.CreateTopics(
		nil,
		[]kafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}},
		nil,
	)
	if err != nil {
		fmt.Printf("Failed to create topic: %v\n", err)
		return
	}

	for _, result := range results {
		if result.Error.Code() == kafka.ErrTopicAlreadyExists {
			fmt.Printf("Topic %s already exists\n", result.Topic)
		} else if result.Error.Code() != kafka.ErrNoError {
			fmt.Printf("Failed to create topic %s: %v\n", result.Topic, result.Error)
		} else {
			fmt.Printf("Topic %s created successfully\n", result.Topic)
		}
	}
}

func orderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Only POST method is allowed.", http.StatusMethodNotAllowed)
		return
	}

	var order Order
	err := json.NewDecoder(r.Body).Decode(&order)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	msg, err := json.Marshal(order)
	if err != nil {
		http.Error(w, "Error encoding data", http.StatusInternalServerError)
		return
	}

	err = prod.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &kafkaTopic,
			Partition: kafka.PartitionAny,
		},
		Value: msg,
	}, nil)
	if err != nil {
		http.Error(w, "Error producing message", http.StatusInternalServerError)
		return
	}
	fmt.Println("messgae received: ", string(msg))
	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "Order processed")
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	kafkaServer = os.Getenv("KAFKA_SERVER")
	kafkaTopic = os.Getenv("KAFKA_TOPIC")

	if kafkaServer == "" || kafkaTopic == "" {
		log.Fatalf("Kafka server or topic is not set in environment variables")
	}

	// createTopic(kafkaServer, kafkaTopic)

	prod, err = kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaServer,
	})
	if err != nil {
		panic(err)
	}
	defer prod.Close()

	http.HandleFunc("/order", orderHandler)
	http.ListenAndServe(":8080", nil)
}
