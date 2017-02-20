package main

import (
	"net/http"
	"os"
	"os/signal"

	"github.com/ONSdigital/dp-dd-csv-filter/config"
	"github.com/ONSdigital/dp-dd-csv-filter/handlers"
	"github.com/ONSdigital/dp-dd-csv-filter/message"
	"github.com/ONSdigital/go-ns/log"
	"github.com/bsm/sarama-cluster"
	"github.com/gorilla/pat"
	"github.com/Shopify/sarama"
)

func main() {
	config.Load()

	// Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	go func() {
		<-signals

		log.Debug("Graceful shutdown was successful.", nil)
		os.Exit(0)
	}()

	go func() {
		router := pat.New()
		router.Post("/filter", handlers.Handle)
		if err := http.ListenAndServe(config.BindAddr, router); err != nil {
			log.Error(err, nil)
			os.Exit(1)
		}
	}()

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Retry.Max = 5
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Producer.Return.Errors = true

	producer, err := sarama.NewSyncProducer([]string{config.KafkaAddr}, kafkaConfig)
	if err != nil {
		log.Error(err, log.Data{"message": "Failed to create message producer."})
		os.Exit(1)
	}
	handlers.SetProducer(producer)

	consumerConfig := cluster.NewConfig()
	consumer, err := cluster.NewConsumer([]string{config.KafkaAddr}, config.KafkaConsumerGroup, []string{config.KafkaConsumerTopic}, consumerConfig)
	if err != nil {
		log.Error(err, nil)
		os.Exit(1)
	}
	message.ConsumerLoop(consumer, handlers.HandleRequest)

}
