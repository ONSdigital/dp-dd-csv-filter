package handlers

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/ONSdigital/dp-dd-csv-filter/ons_aws"
	"github.com/ONSdigital/dp-dd-csv-filter/message/event"
	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
)

var mutex = &sync.Mutex{}

const PANIC_MESSAGE = "Panic!!!"

// MockAWSCli mock implementation of ons_aws.Client
type MockAWSCli struct {
	requestedFiles map[string]int
	savedFiles     map[string]int
	fileBytes      []byte
	err            error
}

func newMockAwsClient() *MockAWSCli {
	mock := &MockAWSCli{requestedFiles: make(map[string]int), savedFiles: make(map[string]int)}
	setAWSClient(mock)
	return mock
}

func (mock *MockAWSCli) GetCSV(requestId string, fileURI ons_aws.S3URL) (io.ReadCloser, error) {
	mutex.Lock()
	defer mutex.Unlock()

	mock.requestedFiles[fileURI.String()]++
	return ioutil.NopCloser(bytes.NewReader(mock.fileBytes)), mock.err
}

func (mock *MockAWSCli) SaveFile(requestId string, reader io.Reader, filePath ons_aws.S3URL) error {
	mutex.Lock()
	defer mutex.Unlock()

	mock.savedFiles[filePath.String()]++
	return nil
}

func (mock *MockAWSCli) getTotalInvocations() int {
	var count = 0
	for _, val := range mock.requestedFiles {
		count += val
	}
	return count
}

func (mock *MockAWSCli) getInvocationsByURI(uri string) int {
	return mock.requestedFiles[uri]
}

func (mock *MockAWSCli) countOfSaveInvocations(uri string) int {
	return mock.savedFiles[uri]
}

// MockCSVProcessor
type MockCSVProcessor struct {
	invocations int
	shouldPanic bool
}

func newMockCSVProcessor() *MockCSVProcessor {
	mock := &MockCSVProcessor{invocations: 0}
	setCSVProcessor(mock)
	return mock
}

// Process mock implementation of the Process function.
func (p *MockCSVProcessor) Process(requestId string, r io.Reader, w io.Writer, d map[string][]string) {
	mutex.Lock()
	defer mutex.Unlock()
	p.invocations++
	if p.shouldPanic {
		panic(PANIC_MESSAGE)
	}
}

// MockProducer
type MockProducer struct {
	sentMessages            []string
	sendMessageError        error
	sendMessagesInvocations int
	messageTopics           []string
}

func newMockProducer() *MockProducer {
	mock := MockProducer{}
	return &mock
}

func (p *MockProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	messageText, _ := msg.Value.Encode()
	p.sentMessages = append(p.sentMessages, string(messageText))
	p.messageTopics = append(p.messageTopics, msg.Topic)
	return 0, 0, p.sendMessageError
}

func (p *MockProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	p.sendMessagesInvocations++
	return errors.New("Should not be calling SendMessages!")
}

func (p *MockProducer) Close() error {
	return nil
}

var filterBucket = "filter-bucket"
var topicName = "transform-topic"

func TestHandler(t *testing.T) {

	Convey("Should invoke AWSClient once with the filter file path.", t, func() {
		recorder := httptest.NewRecorder()
		mockAWSCli, mockCSVProcessor, mockProducer := setMocks(ioutil.ReadAll)

		inputFile := "s3://input-bucket/test.csv"
		outputFile := "s3://transform-bucket/test.out"
		filterFile := "s3://filter-bucket/test.out"
		dimensions := map[string][]string{"dim": {"foo"}}
		filterRequest := createFilterRequest(inputFile, outputFile, dimensions)

		Handle(recorder, createRequest(filterRequest))

		splitterResponse, status := extractResponseBody(recorder)

		So(splitterResponse, ShouldResemble, filterResponseSuccess)
		So(status, ShouldResemble, http.StatusOK)
		So(1, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(1, ShouldEqual, mockAWSCli.getInvocationsByURI(inputFile))
		So(1, ShouldEqual, mockAWSCli.countOfSaveInvocations(filterFile))
		So(0, ShouldEqual, mockAWSCli.countOfSaveInvocations(outputFile))
		So(1, ShouldEqual, mockCSVProcessor.invocations)
		So(1, ShouldEqual, len(mockProducer.sentMessages))
		So(mockProducer.sentMessages[0], ShouldContainSubstring, filterFile)
		So(mockProducer.sentMessages[0], ShouldContainSubstring, outputFile)
		So(mockProducer.messageTopics[0], ShouldEqual, topicName)
	})

	Convey("Should return appropriate error if cannot unmarshall the request body into a FilterRequest.", t, func() {
		recorder := httptest.NewRecorder()
		mockAWSCli, mockCSVProcessor, mockProducer := setMocks(ioutil.ReadAll)

		Handle(recorder, createRequest("This is not a FilterRequest"))

		splitterResponse, status := extractResponseBody(recorder)

		So(0, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(0, ShouldEqual, mockCSVProcessor.invocations)
		So(0, ShouldEqual, len(mockProducer.sentMessages))
		So(splitterResponse, ShouldResemble, filterRespUnmarshalBody)
		So(status, ShouldResemble, http.StatusBadRequest)
	})

	Convey("Should return appropriate error if the awsClient returns an error.", t, func() {
		recorder := httptest.NewRecorder()
		uri := "s3://bucket/target.csv"
		awsErrMsg := "THIS IS AN AWS ERROR"

		mockAWSCli, mockCSVProcessor, mockProducer := setMocks(ioutil.ReadAll)
		mockAWSCli.err = errors.New(awsErrMsg)

		Handle(recorder, createRequest(createFilterRequest(uri, uri, nil)))
		splitterResponse, status := extractResponseBody(recorder)

		So(1, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(1, ShouldEqual, mockAWSCli.getInvocationsByURI(uri))
		So(0, ShouldEqual, mockCSVProcessor.invocations)
		So(0, ShouldEqual, len(mockProducer.sentMessages))
		So(splitterResponse, ShouldResemble, FilterResponse{awsErrMsg})
		So(status, ShouldResemble, http.StatusBadRequest)
	})

	Convey("Should return success response for happy path scenario", t, func() {
		recorder := httptest.NewRecorder()
		inputUri := "s3://input-bucket/target.csv"
		outputUri := "s3://output-bucket/target.csv"
		filterUri := "s3://filter-bucket/target.csv"

		mockAWSCli, mockCSVProcessor, mockProducer := setMocks(ioutil.ReadAll)

		Handle(recorder, createRequest(createFilterRequest(inputUri, outputUri, nil)))
		splitterResponse, statusCode := extractResponseBody(recorder)

		So(1, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(1, ShouldEqual, mockAWSCli.getInvocationsByURI(inputUri))
		So(1, ShouldEqual, mockCSVProcessor.invocations)
		So(1, ShouldEqual, len(mockProducer.sentMessages))
		So(1, ShouldEqual, mockAWSCli.countOfSaveInvocations(filterUri))
		So(0, ShouldEqual, mockAWSCli.countOfSaveInvocations(outputUri))
		So(1, ShouldEqual, len(mockProducer.sentMessages))
		So(mockProducer.sentMessages[0], ShouldContainSubstring, filterUri)
		So(mockProducer.sentMessages[0], ShouldContainSubstring, outputUri)
		So(mockProducer.messageTopics[0], ShouldEqual, topicName)
		So(splitterResponse, ShouldResemble, filterResponseSuccess)
		So(statusCode, ShouldResemble, http.StatusOK)
	})

	Convey("Should handle a bucket path with or without a trailing slash", t, func() {
		recorder := httptest.NewRecorder()
		inputUri := "s3://input-bucket/target.csv"
		outputUri := "s3://output-bucket/target.csv"
		filterUri := "s3://filter-bucket/target.csv"

		setOutputS3Bucket("filter-bucket/")
		mockAWSCli, mockCSVProcessor, mockProducer := setMocks(ioutil.ReadAll)

		Handle(recorder, createRequest(createFilterRequest(inputUri, outputUri, nil)))
		splitterResponse, statusCode := extractResponseBody(recorder)

		So(1, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(1, ShouldEqual, mockAWSCli.getInvocationsByURI(inputUri))
		So(1, ShouldEqual, mockCSVProcessor.invocations)
		So(1, ShouldEqual, len(mockProducer.sentMessages))
		So(1, ShouldEqual, mockAWSCli.countOfSaveInvocations(filterUri))
		So(0, ShouldEqual, mockAWSCli.countOfSaveInvocations(outputUri))
		So(1, ShouldEqual, len(mockProducer.sentMessages))
		So(mockProducer.sentMessages[0], ShouldContainSubstring, filterUri)
		So(mockProducer.sentMessages[0], ShouldContainSubstring, outputUri)
		So(mockProducer.messageTopics[0], ShouldEqual, topicName)
		So(splitterResponse, ShouldResemble, filterResponseSuccess)
		So(statusCode, ShouldResemble, http.StatusOK)
	})

	Convey("Should return appropriate error for unsupported file types", t, func() {
		recorder := httptest.NewRecorder()
		uri := "s3://bucket/unsupported.txt"

		mockAWSCli, mockCSVProcessor, mockProducer := setMocks(ioutil.ReadAll)

		Handle(recorder, createRequest(createFilterRequest(uri, uri, nil)))

		splitterResponse, status := extractResponseBody(recorder)
		So(0, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(0, ShouldEqual, mockCSVProcessor.invocations)
		So(0, ShouldEqual, len(mockProducer.sentMessages))
		So(splitterResponse, ShouldResemble, filterRespUnsupportedFileType)
		So(status, ShouldResemble, http.StatusBadRequest)
	})

	Convey("Should handle a panic.", t, func() {
		recorder := httptest.NewRecorder()
		mockAWSCli, mockCSVProcessor, mockProducer := setMocks(ioutil.ReadAll)

		inputFile := "s3://bucket/test.csv"
		outputFile := "s3://bucket/test.out"
		dimensions := map[string][]string{"dim": {"foo"}}
		filterRequest := createFilterRequest(inputFile, outputFile, dimensions)

		mockCSVProcessor.shouldPanic = true

		Handle(recorder, createRequest(filterRequest))

		splitterResponse, status := extractResponseBody(recorder)

		So(splitterResponse, ShouldResemble, FilterResponse{PANIC_MESSAGE})
		So(status, ShouldResemble, http.StatusBadRequest)
		So(1, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(1, ShouldEqual, mockAWSCli.getInvocationsByURI(inputFile))
		So(0, ShouldEqual, mockAWSCli.countOfSaveInvocations(outputFile))
		So(1, ShouldEqual, mockCSVProcessor.invocations)
		So(0, ShouldEqual, len(mockProducer.sentMessages))
	})

}

func TestGetFilterS3Url(t *testing.T) {
	outputUrl, _ := ons_aws.NewS3URL("s3://output-bucket/folder/filename.csv")
	Convey("Should return appropriate error if s3Url cannot be created.", t, func() {
		setOutputS3Bucket("invalid s3 bucket")
		_, err := getFilterS3Url(outputUrl)
		So(err, ShouldNotBeNil)
	})
	Convey("Should return s3 url when bucket includes s3://", t, func() {
		setOutputS3Bucket("s3://valid-bucket")
		s3, err := getFilterS3Url(outputUrl)
		So(err, ShouldBeNil)
		result := s3.String()
		So(result, ShouldEqual, "s3://valid-bucket/filename.csv")
	})
	Convey("Should return s3 url when bucket does not include s3://", t, func() {
		setOutputS3Bucket("valid-bucket/")
		s3, err := getFilterS3Url(outputUrl)
		So(err, ShouldBeNil)
		result := s3.String()
		So(result, ShouldEqual, "s3://valid-bucket/filename.csv")
	})
	Convey("Should return s3 url when bucket includes path and trailing /", t, func() {
		setOutputS3Bucket("valid-bucket/valid-folder/")
		s3, err := getFilterS3Url(outputUrl)
		So(err, ShouldBeNil)
		result := s3.String()
		So(result, ShouldEqual, "s3://valid-bucket/valid-folder/filename.csv")
	})
}

func extractResponseBody(rec *httptest.ResponseRecorder) (FilterResponse, int) {
	var actual = &FilterResponse{}
	json.Unmarshal([]byte(rec.Body.String()), actual)
	return *actual, rec.Code
}

func createRequest(body interface{}) *http.Request {
	b, _ := json.Marshal(body)
	request, _ := http.NewRequest("POST", "/filter", bytes.NewBuffer(b))
	return request
}

func createFilterRequest(input string, output string, dimensions map[string][]string) event.FilterRequest {
	req, err := event.NewFilterRequest("requestId", input, output, dimensions)
	if err != nil {
		panic(err)
	}
	return req
}

func setMocks(reader requestBodyReader) (*MockAWSCli, *MockCSVProcessor, *MockProducer) {
	mockAWSCli := newMockAwsClient()
	mockCSVProcessor := newMockCSVProcessor()
	mockProducer := newMockProducer()
	SetProducer(mockProducer)
	setReader(reader)
	setOutputS3Bucket(filterBucket)
	setTransformTopic(topicName)
	return mockAWSCli, mockCSVProcessor, mockProducer
}
