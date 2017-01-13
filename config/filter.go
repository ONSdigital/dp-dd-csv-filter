package config

import (
	"github.com/ONSdigital/go-ns/log"
	"os"
)

const bindAddrKey = "BIND_ADDR"
const kafkaAddrKey = "KAFKA_ADDR"
const kafkaConsumerGroup = "KAFKA_CONSUMER_GROUP"
const kafkaConsumerTopic = "KAFKA_CONSUMER_TOPIC"
const awsRegionKey = "AWS_REGION"

// BindAddr the address to bind to.
var BindAddr = ":21100"

// KafkaAddr the Kafka address to send messages to.
var KafkaAddr = "localhost:9092"

// AWSRegion the AWS region to use.
var AWSRegion = "eu-west-1"

// KafkaConsumerGroup the consumer group to consume messages from.
var KafkaConsumerGroup = "filter-request"

// KafkaConsumerTopic the name of the topic to consume messages from.
var KafkaConsumerTopic = "filter-request"

func init() {
	if bindAddrEnv := os.Getenv(bindAddrKey); len(bindAddrEnv) > 0 {
		BindAddr = bindAddrEnv
	}

	if kafkaAddrEnv := os.Getenv(kafkaAddrKey); len(kafkaAddrEnv) > 0 {
		KafkaAddr = kafkaAddrEnv
	}

	if awsRegionEnv := os.Getenv(awsRegionKey); len(awsRegionEnv) > 0 {
		AWSRegion = awsRegionEnv
	}

	if consumerGroupEnv := os.Getenv(kafkaConsumerGroup); len(consumerGroupEnv) > 0 {
		KafkaConsumerGroup = consumerGroupEnv
	}

	if consumerTopicEnv := os.Getenv(kafkaConsumerTopic); len(consumerTopicEnv) > 0 {
		KafkaConsumerTopic = consumerTopicEnv
	}

}

func Load() {
	// Will call init().
	log.Debug("dp-csv-filter Configuration", log.Data{
		bindAddrKey:        BindAddr,
		kafkaAddrKey:       KafkaAddr,
		awsRegionKey:       AWSRegion,
		kafkaConsumerGroup: KafkaConsumerGroup,
		kafkaConsumerTopic: KafkaConsumerTopic,
	})
}
