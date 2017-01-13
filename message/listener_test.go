package message_test

import (
	"encoding/json"
	"github.com/ONSdigital/dp-dd-csv-filter/handlers"
	"github.com/ONSdigital/dp-dd-csv-filter/message"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

var messagesProcessed = 0

func mockFilterFunc(filterRequest handlers.FilterRequest) handlers.FilterResponse {
	messagesProcessed++
	return handlers.FilterResponse{Message: "done"}
}

func TestProcessor(t *testing.T) {
	event := handlers.FilterRequest{
		InputFilePath:  "inputUrl",
		OutputFilePath: "outputUrl",
		Dimensions: map[string][]string{
			"NACE": {
				"08 - Other mining and quarrying",
				"1012 - Processing and preserving of poultry meat",
			},
			"Prodcom Elements": {
				"Work done",
				"Waste Products",
			},
		},
	}

	messageJson, _ := json.Marshal(event)
	topicName := "filter-request"
	mockConsumer := mocks.NewConsumer(t, nil)
	mockConsumer.ExpectConsumePartition(topicName, 0, 0).YieldMessage(&sarama.ConsumerMessage{Value: []byte(messageJson)})

	mockListener := newMocklistener(mockConsumer, topicName)

	Convey("Given a mock consumer and filterer", t, func() {
		messagesProcessed = 0
		go message.ConsumerLoop(mockListener, mockFilterFunc)
		loop := 0

		// Give this at least 300 milli-seconds to run before asserting the message was processed
		for loop < 3 {
			if messagesProcessed >= 1 {
				break
			}
			time.Sleep(100 * time.Millisecond)
			loop++
		}
		So(messagesProcessed, ShouldEqual, 1)
		mockConsumer.Close()
	})

}

func newMocklistener(consumer *mocks.Consumer, topic string) mockListener {
	partitionConsumer, _ := consumer.ConsumePartition(topic, 0, 0)
	return mockListener{
		messages: partitionConsumer.Messages(),
	}
}

type mockListener struct {
	message.Listener
	messages <-chan *sarama.ConsumerMessage
}

func (listener mockListener) Messages() <-chan *sarama.ConsumerMessage {
	return listener.messages
}
