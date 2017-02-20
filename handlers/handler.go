package handlers

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"fmt"
	"runtime/debug"

	"github.com/ONSdigital/dp-dd-csv-filter/aws"
	"github.com/ONSdigital/dp-dd-csv-filter/config"
	"github.com/ONSdigital/dp-dd-csv-filter/filter"
	"github.com/ONSdigital/dp-dd-csv-filter/message/event"
	"github.com/ONSdigital/go-ns/log"
	"github.com/Shopify/sarama"
	"strings"
)

const csvFileExt = ".csv"

type requestBodyReader func(r io.Reader) ([]byte, error)

// FilterResponse struct defines the response for the /filter API.
type FilterResponse struct {
	Message string `json:"message,omitempty"`
}

// FilterFunc defines a function (implemented by HandleRequest) that performs the filtering requested in a FilterRequest
type FilterFunc func(event.FilterRequest) FilterResponse

var unsupportedFileTypeErr = errors.New("Unspported file type.")
var awsClientErr = errors.New("Error while attempting get to get from from AWS s3 bucket.")
var awsService = aws.NewService()
var csvProcessor filter.CSVProcessor = filter.NewCSVProcessor()
var readFilterRequestBody requestBodyReader = ioutil.ReadAll

// Responses
var filterRespReadReqBodyErr = FilterResponse{"Error when attempting to read request body."}
var filterRespUnmarshalBody = FilterResponse{"Error when attempting to unmarshal request body."}
var filterRespUnsupportedFileType = FilterResponse{"Unspported file type. Please specify a filePath for a .csv file."}
var filterResponseSuccess = FilterResponse{"Your request is being processed."}

var producer sarama.SyncProducer
var outputS3Bucket = config.OutputS3Bucket
var transformTopic = config.KafkaTransformTopic

// Handle CSV filter handler. Get the requested file from AWS S3, filter it to a temporary file, upload the temporary file to the filter bucket, send a message to request the file is transformed..
func Handle(w http.ResponseWriter, req *http.Request) {
	bytes, err := readFilterRequestBody(req.Body)
	defer req.Body.Close()

	if err != nil {
		log.Error(err, nil)
		WriteResponse(w, filterRespReadReqBodyErr, http.StatusBadRequest)
		return
	}

	var filterRequest event.FilterRequest
	if err := json.Unmarshal(bytes, &filterRequest); err != nil {
		log.Error(err, nil)
		WriteResponse(w, filterRespUnmarshalBody, http.StatusBadRequest)
		return
	}

	response := HandleRequest(filterRequest)
	status := http.StatusBadRequest
	if response == filterResponseSuccess {
		status = http.StatusOK
	}
	WriteResponse(w, response, status)
}

// Performs the filtering as specified in the FilterRequest, returning a FilterResponse
func HandleRequest(filterRequest event.FilterRequest) (resp FilterResponse) {

	if fileType := filepath.Ext(filterRequest.InputURL.GetFilePath()); fileType != csvFileExt {
		log.Error(unsupportedFileTypeErr, log.Data{"expected": csvFileExt, "actual": fileType})
		return filterRespUnsupportedFileType
	}

	awsReader, err := awsService.GetCSV(filterRequest.InputURL)
	if err != nil {
		log.Error(awsClientErr, log.Data{"details": err.Error()})
		return FilterResponse{err.Error()}
	}

	outputFileLocation := "/var/tmp/csv_filter_" + strconv.Itoa(time.Now().Nanosecond()) + ".csv"
	outputFile, err := os.Create(outputFileLocation)
	if err != nil {
		log.Error(err, log.Data{"message": "Error creating temp output file in location " + outputFileLocation})
		panic(err)
	}

	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Something went wrong: %s\n%s", r, debug.Stack())
			message := fmt.Sprintf("%s", r)
			resp = FilterResponse{message}
		}
	}()

	csvProcessor.Process(awsReader, bufio.NewWriter(outputFile), filterRequest.Dimensions)

	filterUrl, err := getFilterS3Url(filterRequest.OutputURL)
	if err != nil {
		log.Error(err, log.Data{"message": "Failed to get tmp output file for s3 uploading!"})
		return FilterResponse{"Unable to obtain filter s3 url to send filtered file to: " + err.Error()}
	}

	tmpFile, err := os.Open(outputFileLocation)
	if err != nil {
		log.Error(err, log.Data{"message": "Failed to get tmp output file for s3 uploading!"})
	}

	awsService.SaveFile(bufio.NewReader(tmpFile), filterUrl)

	os.Remove(outputFileLocation)

	sendTransformMessage(filterRequest, filterUrl)

	return filterResponseSuccess
}

func getFilterS3Url(outputUrl aws.S3URL) (aws.S3URL, error) {
	path := outputUrl.GetFilePath()
	tokens := strings.Split(path, "/")
	filename := tokens[len(tokens)-1]
	filterUrlString := outputS3Bucket
	if !strings.HasPrefix(filterUrlString, "s3://") {
		filterUrlString = "s3://" + filterUrlString
	}
	if !strings.HasSuffix(filterUrlString, "/") {
		filterUrlString = filterUrlString + "/"
	}
	return aws.NewS3URL(filterUrlString + filename)
}

func sendTransformMessage(filterRequest event.FilterRequest, filterUrl aws.S3URL) {
	message := event.NewTransformRequest(filterUrl, filterRequest.OutputURL, filterRequest.RequestID)

	messageJSON, err := json.Marshal(message)
	if err != nil {
		log.ErrorC(filterRequest.RequestID, err, log.Data{
			"details": "Could not create the json representation of message",
			"message": messageJSON,
		})
		panic(err)
	}

	producerMsg := &sarama.ProducerMessage{
		Topic: transformTopic,
		Value: sarama.ByteEncoder(messageJSON),
	}

	log.Debug("Sending transformRequest message", log.Data{"message": messageJSON})
	_, _, err = producer.SendMessage(producerMsg)
	if err != nil {
		log.Error(err, log.Data{
			"details": "Failed to add messages to Kafka",
		})
	}
}

func setReader(reader requestBodyReader) {
	readFilterRequestBody = reader
}

func setCSVProcessor(p filter.CSVProcessor) {
	csvProcessor = p
}

func setAWSClient(c aws.AWSService) {
	awsService = c
}

func SetProducer(p sarama.SyncProducer) {
	producer = p
}

func setOutputS3Bucket(o string) {
	outputS3Bucket = o
}

func setTransformTopic(t string) {
	transformTopic = t
}
