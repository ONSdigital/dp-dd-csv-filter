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

	"github.com/ONSdigital/dp-dd-csv-filter/aws"
	"github.com/ONSdigital/dp-dd-csv-filter/message/event"
	. "github.com/smartystreets/goconvey/convey"
)

var mutex = &sync.Mutex{}

const PANIC_MESSAGE = "Panic!!!"

// MockAWSCli mock implementation of aws.Client
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

func (mock *MockAWSCli) GetCSV(fileURI aws.S3URL) (io.Reader, error) {
	mutex.Lock()
	defer mutex.Unlock()

	mock.requestedFiles[fileURI.String()]++
	return bytes.NewReader(mock.fileBytes), mock.err
}

func (mock *MockAWSCli) SaveFile(reader io.Reader, filePath aws.S3URL) error {
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
func (p *MockCSVProcessor) Process(r io.Reader, w io.Writer, d map[string][]string) {
	mutex.Lock()
	defer mutex.Unlock()
	p.invocations++
	if (p.shouldPanic) {
		panic(PANIC_MESSAGE)
	}
}

func TestHandler(t *testing.T) {

	Convey("Should invoke AWSClient once with the request file path.", t, func() {
		recorder := httptest.NewRecorder()
		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)

		inputFile := "s3://bucket/test.csv"
		outputFile := "s3://bucket/test.out"
		dimensions := map[string][]string{"dim": {"foo"}}
		filterRequest := createFilterRequest(inputFile, outputFile, dimensions)

		Handle(recorder, createRequest(filterRequest))

		splitterResponse, status := extractResponseBody(recorder)

		So(splitterResponse, ShouldResemble, filterResponseSuccess)
		So(status, ShouldResemble, http.StatusOK)
		So(1, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(1, ShouldEqual, mockAWSCli.getInvocationsByURI(inputFile))
		So(1, ShouldEqual, mockAWSCli.countOfSaveInvocations(outputFile))
		So(1, ShouldEqual, mockCSVProcessor.invocations)
	})

	Convey("Should return appropriate error if cannot unmarshall the request body into a FilterRequest.", t, func() {
		recorder := httptest.NewRecorder()
		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)

		Handle(recorder, createRequest("This is not a FilterRequest"))

		splitterResponse, status := extractResponseBody(recorder)

		So(0, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(0, ShouldEqual, mockCSVProcessor.invocations)
		So(splitterResponse, ShouldResemble, filterRespUnmarshalBody)
		So(status, ShouldResemble, http.StatusBadRequest)
	})

	Convey("Should return appropriate error if the awsClient returns an error.", t, func() {
		recorder := httptest.NewRecorder()
		uri := "s3://bucket/target.csv"
		awsErrMsg := "THIS IS AN AWS ERROR"

		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)
		mockAWSCli.err = errors.New(awsErrMsg)

		Handle(recorder, createRequest(createFilterRequest(uri, uri, nil)))
		splitterResponse, status := extractResponseBody(recorder)

		So(1, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(1, ShouldEqual, mockAWSCli.getInvocationsByURI(uri))
		So(0, ShouldEqual, mockCSVProcessor.invocations)
		So(splitterResponse, ShouldResemble, FilterResponse{awsErrMsg})
		So(status, ShouldResemble, http.StatusBadRequest)
	})

	Convey("Should return success response for happy path scenario", t, func() {
		recorder := httptest.NewRecorder()
		uri := "s3://bucket/target.csv"

		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)

		Handle(recorder, createRequest(createFilterRequest(uri, uri, nil)))
		splitterResponse, statusCode := extractResponseBody(recorder)

		So(1, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(1, ShouldEqual, mockAWSCli.getInvocationsByURI(uri))
		So(1, ShouldEqual, mockCSVProcessor.invocations)
		So(splitterResponse, ShouldResemble, filterResponseSuccess)
		So(statusCode, ShouldResemble, http.StatusOK)
	})

	Convey("Should return appropriate error for unsupported file types", t, func() {
		recorder := httptest.NewRecorder()
		uri := "s3://bucket/unsupported.txt"

		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)

		Handle(recorder, createRequest(createFilterRequest(uri, uri, nil)))

		splitterResponse, status := extractResponseBody(recorder)
		So(0, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(0, ShouldEqual, mockCSVProcessor.invocations)
		So(splitterResponse, ShouldResemble, filterRespUnsupportedFileType)
		So(status, ShouldResemble, http.StatusBadRequest)
	})


	Convey("Should handle a panic.", t, func() {
		recorder := httptest.NewRecorder()
		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)

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
	req, err := event.NewFilterRequest(input, output, dimensions)
	if err != nil {
		panic(err)
	}
	return req
}

func setMocks(reader requestBodyReader) (*MockAWSCli, *MockCSVProcessor) {
	mockAWSCli := newMockAwsClient()
	mockCSVProcessor := newMockCSVProcessor()
	setReader(reader)
	return mockAWSCli, mockCSVProcessor
}
