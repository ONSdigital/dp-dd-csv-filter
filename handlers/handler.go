package handlers

import (
	"encoding/json"
	"github.com/ONSdigital/dp-dd-csv-filter/aws"
	"io"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"errors"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/dp-dd-csv-filter/filter"
)

const csvFileExt = ".csv"

type requestBodyReader func(r io.Reader) ([]byte, error)

// FilterResponse struct defines the response for the /filter API.
type FilterResponse struct {
	Message string `json:"message,omitempty"`
}

// FilterRequest struct defines a filter request
type FilterRequest struct {
	FilePath string `json:"filePath"`
}

var unsupportedFileTypeErr = errors.New("Unspported file type.")
var awsClientErr = errors.New("Error while attempting get to get from from AWS s3 bucket.")
var filePathParamMissingErr = errors.New("No filePath value was provided.")
var awsService = aws.NewService()
var csvProcessor filter.CSVProcessor = filter.NewCSVProcessor()
var readFilterRequestBody requestBodyReader = ioutil.ReadAll

// Responses
var filterRespReadReqBodyErr = FilterResponse{"Error when attempting to read request body."}
var filterRespUnmarshalBody = FilterResponse{"Error when attempting to unmarshal request body."}
var filterRespFilePathMissing = FilterResponse{"No filePath parameter was specified in the request body."}
var filterRespUnsupportedFileType = FilterResponse{"Unspported file type. Please specify a filePath for a .csv file."}
var filterResponseSuccess = FilterResponse{"Your request is being processed."}

// Handle CSV splitter handler. Get the requested file from AWS S3, split it and send each row to the configured Kafka Topic.
func Handle(w http.ResponseWriter, req *http.Request) {
	bytes, err := readFilterRequestBody(req.Body)
	defer req.Body.Close()

	if err != nil {
		log.Error(err, nil)
		WriteResponse(w, filterRespReadReqBodyErr, http.StatusBadRequest)
		return
	}

	var splitterReq FilterRequest
	if err := json.Unmarshal(bytes, &splitterReq); err != nil {
		log.Error(err, nil)
		WriteResponse(w, filterRespUnmarshalBody, http.StatusBadRequest)
		return
	}

	if len(splitterReq.FilePath) == 0 {
		log.Error(filePathParamMissingErr, nil)
		WriteResponse(w, filterRespFilePathMissing, http.StatusBadRequest)
		return
	}

	if fileType := filepath.Ext(splitterReq.FilePath); fileType != csvFileExt {
		log.Error(unsupportedFileTypeErr, log.Data{"expected": csvFileExt, "actual": fileType})
		WriteResponse(w, filterRespUnsupportedFileType, http.StatusBadRequest)
		return
	}
	awsReader, err := awsService.GetCSV(splitterReq.FilePath)
	if err != nil {
		log.Error(awsClientErr, log.Data{"details": err.Error()})
		WriteResponse(w, FilterResponse{err.Error()}, http.StatusBadRequest)
		return
	}


	// todo this need the dimensions from the call/message
	csvProcessor.Process(awsReader, nil, nil)




	WriteResponse(w, filterResponseSuccess, http.StatusOK)
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
