package message

import (
	"encoding/json"

	"fmt"

	"github.com/ONSdigital/dp-dd-csv-filter/handlers"
	"github.com/ONSdigital/dp-dd-csv-filter/message/event"
	"github.com/ONSdigital/go-ns/log"
	"github.com/Shopify/sarama"
)

func ConsumerLoop(listener Listener, filterer handlers.FilterFunc) {
	for message := range listener.Messages() {
		log.Debug("Message received from Kafka: "+string(message.Value), nil)
		processMessage(message, filterer)
	}
}

func processMessage(message *sarama.ConsumerMessage, filterer handlers.FilterFunc) error {

	var filterRequest event.FilterRequest
	if err := json.Unmarshal(message.Value, &filterRequest); err != nil {
		log.Error(err, nil)
		return err
	}

	log.Debug(fmt.Sprintf("About to process:%s", filterRequest.String()), nil)
	filterer(filterRequest)
	log.Debug(fmt.Sprintf("Finished processing:%s", filterRequest.String()), nil)

	return nil
}

type Listener interface {
	Messages() <-chan *sarama.ConsumerMessage
}
