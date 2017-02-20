package config

import (
	"os"

	"github.com/ONSdigital/go-ns/log"
)

const bindAddrKey = "BIND_ADDR"
const kafkaAddrKey = "KAFKA_ADDR"
const kafkaConsumerGroupKey = "KAFKA_CONSUMER_GROUP"
const kafkaConsumerTopicKey = "KAFKA_CONSUMER_TOPIC"
const awsRegionKey = "AWS_REGION"
const outputS3BucketKey = "OUTPUT_S3_BUCKET"
const kafkaTransformTopicKey = "KAFKA_TRANSFORM_TOPIC"

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

// KafkaTransformTopic the name of the topic to send transform request messages to.
var KafkaTransformTopic = "transform-request"

// OutputS3Bucket the name of the bucket to send filtered csv files to
var OutputS3Bucket = "dp-dd-csv-filter/" + os.Getenv("USER") + "/filtered/"

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

	if consumerGroupEnv := os.Getenv(kafkaConsumerGroupKey); len(consumerGroupEnv) > 0 {
		KafkaConsumerGroup = consumerGroupEnv
	}

	if consumerTopicEnv := os.Getenv(kafkaConsumerTopicKey); len(consumerTopicEnv) > 0 {
		KafkaConsumerTopic = consumerTopicEnv
	}

	if transformTopicEnv := os.Getenv(kafkaTransformTopicKey); len(transformTopicEnv) > 0 {
		KafkaTransformTopic = transformTopicEnv
	}

	if s3BucketEnv := os.Getenv(outputS3BucketKey); len(s3BucketEnv) > 0 {
		OutputS3Bucket = s3BucketEnv
	}

}

func Load() {
	// Will call init().
	log.Debug("dp-csv-filter Configuration", log.Data{
		bindAddrKey:            BindAddr,
		kafkaAddrKey:           KafkaAddr,
		awsRegionKey:           AWSRegion,
		kafkaConsumerGroupKey:  KafkaConsumerGroup,
		kafkaConsumerTopicKey:  KafkaConsumerTopic,
		kafkaTransformTopicKey: KafkaTransformTopic,
		outputS3BucketKey:      OutputS3Bucket,
	})
}
