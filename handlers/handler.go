package handlers

import (
	"bufio"
	"encoding/json"
	"errors"
	"github.com/ONSdigital/dp-dd-csv-filter/aws"
	"github.com/ONSdigital/dp-dd-csv-filter/filter"
	"github.com/ONSdigital/go-ns/log"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const csvFileExt = ".csv"

type requestBodyReader func(r io.Reader) ([]byte, error)

// FilterResponse struct defines the response for the /filter API.
type FilterResponse struct {
	Message string `json:"message,omitempty"`
}

// FilterRequest struct defines a filter request
type FilterRequest struct {
	InputFilePath  string              `json:"inputFilePath"`
	OutputFilePath string              `json:"outputFilePath"`
	Dimensions     map[string][]string `json:"dimensions"`
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

// Handle CSV filter handler. Get the requested file from AWS S3, filter it to a temporary file, then upload the temporary file.
func Handle(w http.ResponseWriter, req *http.Request) {
	bytes, err := readFilterRequestBody(req.Body)
	defer req.Body.Close()

	if err != nil {
		log.Error(err, nil)
		WriteResponse(w, filterRespReadReqBodyErr, http.StatusBadRequest)
		return
	}

	var filterRequest FilterRequest
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

func HandleRequest(filterRequest FilterRequest) FilterResponse {
	if len(filterRequest.InputFilePath) == 0 {
		log.Error(filePathParamMissingErr, nil)
		return filterRespFilePathMissing
	}

	if fileType := filepath.Ext(filterRequest.InputFilePath); fileType != csvFileExt {
		log.Error(unsupportedFileTypeErr, log.Data{"expected": csvFileExt, "actual": fileType})
		return filterRespUnsupportedFileType
	}

	awsReader, err := awsService.GetCSV(filterRequest.InputFilePath)
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

	csvProcessor.Process(awsReader, bufio.NewWriter(outputFile), filterRequest.Dimensions)

	tmpFile, err := os.Open(outputFileLocation)
	if err != nil {
		log.Error(err, log.Data{"message": "Failed to get tmp output file for s3 uploading!"})
	}

	awsService.SaveFile(bufio.NewReader(tmpFile), filterRequest.OutputFilePath)

	return filterResponseSuccess
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
