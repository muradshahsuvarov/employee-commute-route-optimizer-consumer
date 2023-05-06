package main

import (
	"fmt"
	"log"

	"github.com/Shopify/sarama"
	"gopkg.in/ini.v1"
)

var msg *sarama.ConsumerMessage

func ConsumeMessages(server string, partition int32, topic string) {
	// Load consumer properties from file
	filePath := "<Path to consumer.properties>"
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
		case err := <-partitionConsumer.Errors():
			// Handle consumer errors
			log.Printf("Error while consuming message: %v\n", err)
		}
	}
}

func main() {

	fmt.Println("Welcome to ECRO Kafka Consumer")

}
