package event

import (
	"github.com/ONSdigital/dp-dd-csv-filter/aws"
	"github.com/ONSdigital/go-ns/log"
)

type FilterRequest struct {
	InputURL   *aws.S3URLType      `json:"inputUrl"`
	OutputURL  *aws.S3URLType      `json:"outputUrl"`
	Dimensions map[string][]string `json:"dimensions"`
}

var NilRequest = FilterRequest{}

func NewFilterRequest(inputUrl string, outputUrl string, dimensions map[string][]string) (FilterRequest, error) {
	var input, output aws.S3URLType
	var err error
	if input, err = aws.NewS3URL(inputUrl); err != nil {
		log.Error(err, log.Data{"Details": "Invalid inputUrl for S3URLType"})
		return NilRequest, err
	}
	if output, err = aws.NewS3URL(outputUrl); err != nil {
		log.Error(err, log.Data{"Details": "Invalid outputUrl for S3URLType"})
		return NilRequest, err
	}
	return FilterRequest{InputURL: &input, OutputURL: &output, Dimensions: dimensions}, nil
}
