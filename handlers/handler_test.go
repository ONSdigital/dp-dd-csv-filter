package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"bytes"
	"errors"
	. "github.com/smartystreets/goconvey/convey"
	"io"
	"io/ioutil"
	"sync"
)

var mutex = &sync.Mutex{}

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

func (mock *MockAWSCli) GetCSV(fileURI string) (io.Reader, error) {
	mutex.Lock()
	defer mutex.Unlock()

	mock.requestedFiles[fileURI]++
	return bytes.NewReader(mock.fileBytes), mock.err
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

func (mock *MockAWSCli) SaveFile(reader io.Reader, filePath string) error {
	mutex.Lock()
	defer mutex.Unlock()

	mock.savedFiles[filePath]++
	return nil
}

// MockCSVProcessor
type MockCSVProcessor struct {
	invocations int
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
}

func TestHandler(t *testing.T) {
	Convey("Should return error response if no filePath parameter is provided.", t, func() {
		recorder := httptest.NewRecorder()
		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)

		Handle(recorder, createRequest(nil))

		splitterResponse, status := extractResponseBody(recorder)

		So(splitterResponse, ShouldResemble, filterRespFilePathMissing)
		So(status, ShouldResemble, http.StatusBadRequest)
		So(0, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(0, ShouldEqual, mockCSVProcessor.invocations)
	})

	Convey("Should invoke AWSClient once with the request file path.", t, func() {
		recorder := httptest.NewRecorder()
		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)

		inputFile := "/test.csv"
		outputFile := "/test.out"
		dimensions := map[string][]string{"dim": {"foo"}}
		Handle(recorder, createRequest(FilterRequest{InputFilePath: inputFile, OutputFilePath: outputFile, Dimensions: dimensions}))

		splitterResponse, status := extractResponseBody(recorder)

		So(splitterResponse, ShouldResemble, filterResponseSuccess)
		So(status, ShouldResemble, http.StatusOK)
		So(1, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(1, ShouldEqual, mockAWSCli.getInvocationsByURI(inputFile))
		So(1, ShouldEqual, mockAWSCli.countOfSaveInvocations(outputFile))
		So(1, ShouldEqual, mockCSVProcessor.invocations)
	})

	Convey("Should return appropriate error if cannot unmarshall the request body into a SplitterRequest.", t, func() {
		recorder := httptest.NewRecorder()
		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)

		Handle(recorder, createRequest("This is not a SplitterRequest"))

		splitterResponse, status := extractResponseBody(recorder)

		So(0, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(0, ShouldEqual, mockCSVProcessor.invocations)
		So(splitterResponse, ShouldResemble, filterRespUnmarshalBody)
		So(status, ShouldResemble, http.StatusBadRequest)
	})

	Convey("Should return appropriate error if request body has empty of missing filePath field.", t, func() {
		recorder := httptest.NewRecorder()
		request := createRequest(FilterRequest{})

		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)

		Handle(recorder, request)

		splitterResponse, status := extractResponseBody(recorder)

		So(0, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(0, ShouldEqual, mockCSVProcessor.invocations)
		So(splitterResponse, ShouldResemble, filterRespFilePathMissing)
		So(status, ShouldResemble, http.StatusBadRequest)
	})

	Convey("Should return appropriate error if the awsClient returns an error.", t, func() {
		recorder := httptest.NewRecorder()
		uri := "/target.csv"
		awsErrMsg := "THIS IS AN AWS ERROR"

		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)
		mockAWSCli.err = errors.New(awsErrMsg)

		Handle(recorder, createRequest(FilterRequest{InputFilePath: uri}))
		splitterResponse, status := extractResponseBody(recorder)

		So(1, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(1, ShouldEqual, mockAWSCli.getInvocationsByURI(uri))
		So(0, ShouldEqual, mockCSVProcessor.invocations)
		So(splitterResponse, ShouldResemble, FilterResponse{awsErrMsg})
		So(status, ShouldResemble, http.StatusBadRequest)
	})

	Convey("Should return success response for happy path scenario", t, func() {
		recorder := httptest.NewRecorder()
		uri := "/target.csv"

		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)

		Handle(recorder, createRequest(FilterRequest{InputFilePath: uri}))
		splitterResponse, statusCode := extractResponseBody(recorder)

		So(1, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(1, ShouldEqual, mockAWSCli.getInvocationsByURI(uri))
		So(1, ShouldEqual, mockCSVProcessor.invocations)
		So(splitterResponse, ShouldResemble, filterResponseSuccess)
		So(statusCode, ShouldResemble, http.StatusOK)
	})

	Convey("Should return appropriate error for unsupported file types", t, func() {
		recorder := httptest.NewRecorder()
		uri := "/unsupported.txt"

		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)

		Handle(recorder, createRequest(FilterRequest{InputFilePath: uri}))

		splitterResponse, status := extractResponseBody(recorder)
		So(0, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(0, ShouldEqual, mockCSVProcessor.invocations)
		So(splitterResponse, ShouldResemble, filterRespUnsupportedFileType)
		So(status, ShouldResemble, http.StatusBadRequest)
	})
}

func extractResponseBody(rec *httptest.ResponseRecorder) (FilterResponse, int) {
	var actual = &FilterResponse{}
	json.Unmarshal([]byte(rec.Body.String()), actual)
	return *actual, rec.Code
}

func createRequest(body interface{}) *http.Request {
	b, _ := json.Marshal(body)
	request, _ := http.NewRequest("POST", "/splitter", bytes.NewBuffer(b))
	return request
}

func setMocks(reader requestBodyReader) (*MockAWSCli, *MockCSVProcessor) {
	mockAWSCli := newMockAwsClient()
	mockCSVProcessor := newMockCSVProcessor()
	setReader(reader)
	return mockAWSCli, mockCSVProcessor
}
