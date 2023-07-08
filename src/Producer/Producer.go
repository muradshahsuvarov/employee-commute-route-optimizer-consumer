package Producer

import (
	"fmt"
	"log"

	"github.com/Shopify/sarama"
	"gopkg.in/ini.v1"
)

func ProduceMessage(_id string, _server string, _topic string, _messageType string, _message string) {

	// The content of the consumer.properties file
	consumerProperties := []byte(`
		bootstrap.servers=localhost:9092
		group.id=test-consumer-group
		auto.offset.reset=latest
	`)
	// Load consumer properties from file
	cfg, err_1 := ini.LoadSources(ini.LoadOptions{},
		consumerProperties)
	if err_1 != nil {
		log.Fatalf("Failed to load consumer properties: %v", err_1)
	}

	// Create Kafka config using the loaded properties
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = cfg.Section("producer").Key("retry.max").MustInt(5)
	config.Producer.Return.Successes = true // Set to true for SyncProducer

	// Create a new Kafka producer using the config
	producer, err := sarama.NewSyncProducer([]string{_server}, config)
	if err != nil {
		log.Printf("Failed to create Kafka producer: %v", err)
		return
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("Failed to close Kafka producer: %v", err)
		}
	}()

	// Send a message to a Kafka topic
	topic := _topic
	message := fmt.Sprintf(`{"id":"%s","type":"%s","message":"%s"}`, _id, _messageType, _message)
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Printf("Failed to send Kafka message: %v", err)
		return
	}

	// Print the message details
	fmt.Printf("Message sent to topic '%s', partition %d, offset %d\n", topic, partition, offset)
}
