package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const MIN_MESSAGE_COUNT = 100

var kafkaServer string
var kafkaConsumerTopic string
var groupId string
var consumer *kafka.Consumer
var dynamoClient *dynamodb.Client

type PizzaMenu struct {
	Name     string `json:"name"`
	Price    int    `json:"price"`
	Quantity int    `json:"Quantity"`
}

type PizzaUncooked struct {
	Id        string    `json:"id"`
	Orderer   string    `json:"orderer"`
	MenuName  string    `json:"menuName"`
	CreatedAt time.Time `json:createdAt`
}

func processMessage(msg *kafka.Message) {
	var pizza PizzaUncooked
	fmt.Printf("received message: %s\n", string(msg.Value))

	err := json.Unmarshal(msg.Value, &pizza)

	if err != nil {
		fmt.Printf("failed to cook the pizze: %s", err)
		return
	}

	item := map[string]types.AttributeValue{
		"id":        &types.AttributeValueMemberS{Value: pizza.Id},
		"orderer":   &types.AttributeValueMemberS{Value: pizza.Orderer},
		"menuName":  &types.AttributeValueMemberS{Value: pizza.MenuName},
		"createdAt": &types.AttributeValueMemberS{Value: pizza.CreatedAt.String()},
	}

	input := &dynamodb.PutItemInput{
		TableName: aws.String("pizza"),
		Item:      item,
	}

	_, err = dynamoClient.PutItem(context.TODO(), input)

	if err != nil {
		fmt.Printf("failed to put item. %s", err)
	}

}

func main() {

	err := godotenv.Load()

	if err != nil {
		panic(err)

	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("ap-northeast-2"))

	if err != nil {
		panic(err)
	}

	dynamoClient = dynamodb.NewFromConfig(cfg)

	kafkaServer = os.Getenv("KAFKA_SERVER")
	kafkaConsumerTopic = os.Getenv("KAFKA_CONSUMER_TOPIC")
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
			go processMessage(et)
			if msgCount&MIN_MESSAGE_COUNT == 0 {
				_, err := consumer.Commit()
				if err != nil {
					log.Printf("failed to process message. %s", et.Value)
				}

			} else {
				log.Printf("committed offsets.")
			}

		case kafka.Error:
			fmt.Println("something went wrong. %s", et.String())

		}
	}

}
