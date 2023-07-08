package main

import (
	"encoding/json"
	"fmt"
	"log"
	"main/src/Producer"
	"main/src/RouteFinder"
	"strings"

	"github.com/Shopify/sarama"
)

var msg *sarama.ConsumerMessage

type RouteRequest struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Message string `json:"message"`
}

func ConsumeMessages(server string, partition int32, topic string) {
	// Create Kafka config using default values
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Version = sarama.V2_6_0_0 // Set the desired Kafka version

	// Create a new Kafka consumer using the config
	consumer, err := sarama.NewConsumer([]string{server}, config)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("Failed to close Kafka consumer: %v", err)
		}
	}()

	// Create a new Kafka consumer partition for the topic
	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to create Kafka partition consumer: %v", err)
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Printf("Failed to close Kafka partition consumer: %v", err)
		}
	}()

	// Start consuming messages from the Kafka topic
	for {
		select {
		case msg = <-partitionConsumer.Messages():
			// Process the received message
			fmt.Printf("Received message: Topic=%s, Partition=%d, Offset=%d, Key=%s, Value=%s\n",
				msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))

			var routeResponse RouteFinder.RouteResponse
			routeRequestObject := RouteRequest{}

			err := json.Unmarshal(msg.Value, &routeRequestObject)
			if err != nil {
				fmt.Printf("Failed to unmarshal msg.Value struct to JSON: %v\n", err)
				return
			}

			url := routeRequestObject.Message

			// Get Route from point A to point B
			routeResponse = routeResponse.GetRouteFromAtoB(url)

			// Convert struct to string using JSON marshaling
			routeResponseJSON, err := json.Marshal(routeResponse)
			if err != nil {
				fmt.Printf("Failed to marshal struct to JSON: %v\n", err)
				return
			}
			routeResponseString := string(routeResponseJSON)
			routeResponseString = strings.ReplaceAll(routeResponseString, "\"", "\\\"")

			// Create a new instance of the RouteRequest struct
			routeRequest := RouteRequest{}
			err = json.Unmarshal(msg.Value, &routeRequest)
			routeRequest.Message = routeResponseString
			if err != nil {
				fmt.Printf("Failed to parse JSON: %v", err)
				return
			}

			// Produce the message using the retrieved producer.properties
			Producer.ProduceMessage(routeRequest.ID, server, "ecro_res_topic", routeRequest.Type, routeRequest.Message)

		case err := <-partitionConsumer.Errors():
			// Handle consumer errors
			log.Printf("Error while consuming message: %v\n", err)
		}
	}
}

func main() {
	fmt.Println("Welcome to ECRO Kafka Consumer")
	ConsumeMessages("localhost:9092", 0, "ecro_req_topic")
}
