package main

import (
	"encoding/json"
	"fmt"
	"log"
	"main/src/Producer"
	"main/src/RouteFinder"

	"github.com/Shopify/sarama"
	"gopkg.in/ini.v1"
)

var msg *sarama.ConsumerMessage

type RouteRequest struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Message string `json:"message"`
}

func ConsumeMessages(server string, partition int32, topic string) {
	// Load consumer properties from file
	filePath := "C:\\kafka\\config\\consumer.properties"
	cfg, err := ini.Load(filePath)
	if err != nil {
		log.Fatalf("Failed to load consumer properties file: %v", err)
	}

	// Create Kafka config using the loaded properties
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Version = sarama.V2_6_0_0 // Set the desired Kafka version

	// Update Kafka config with properties from the file
	config.Net.SASL.Enable = cfg.Section("consumer").Key("sasl.enable").MustBool(false)
	config.Net.SASL.User = cfg.Section("consumer").Key("sasl.username").String()
	config.Net.SASL.Password = cfg.Section("consumer").Key("sasl.password").String()

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

			var routeResponse RouteFinder.RouteResponse = RouteFinder.RouteResponse{}

			// Parsing URL
			routeRequestObject_0 := RouteRequest{}
			err_0 := json.Unmarshal([]byte(string(msg.Value)), &routeRequestObject_0)
			if err_0 != nil {
				fmt.Printf("Failed to marshal msg.Value struct to JSON: %v\n", err)
				return
			}

			url := routeRequestObject_0.Message

			// Get Route from point A to point B
			routeResponse = routeResponse.GetRouteFromAtoB(url)

			// Convert struct to string using JSON marshaling
			routeResponseJSON, err_1 := json.Marshal(routeResponse)
			if err_1 != nil {
				fmt.Printf("Failed to marshal struct to JSON: %v\n", err)
				return
			}

			routeResponseString := string(routeResponseJSON)

			// Create a new instance of the RouteRequest struct
			routeRequest_1 := RouteRequest{}

			// Kafka Consumer Configuration File
			var _consumerPropertiesFile = "C:\\kafka\\config\\consumer.properties"

			// Unmarshal the JSON string into the struct
			err_2 := json.Unmarshal([]byte(string(msg.Value)), &routeRequest_1)

			routeRequest_1.Message = routeResponseString

			if err_2 != nil {
				fmt.Printf("Failed to parse JSON: %v", err)
				return
			}

			Producer.ProduceMessage(routeRequest_1.ID, "localhost:9092", "ecro_res_topic", routeRequest_1.Type, routeRequest_1.Message,
				_consumerPropertiesFile)

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
