package event

import (
	"fmt"

	"github.com/ONSdigital/dp-dd-csv-filter/ons_aws"
	"github.com/ONSdigital/go-ns/log"
)

type FilterRequest struct {
	RequestID  string              `json:"requestId"`
	InputURL   ons_aws.S3URL           `json:"inputUrl"`
	OutputURL  ons_aws.S3URL           `json:"outputUrl"`
	Dimensions map[string][]string `json:"dimensions"`
}

var NilRequest = FilterRequest{}

func NewFilterRequest(requestId string, inputUrl string, outputUrl string, dimensions map[string][]string) (FilterRequest, error) {
	var input, output ons_aws.S3URL
	var err error
	if input, err = ons_aws.NewS3URL(inputUrl); err != nil {
		log.ErrorC(requestId, err, log.Data{"Details": "Invalid inputUrl"})
		return NilRequest, err
	}
	if output, err = ons_aws.NewS3URL(outputUrl); err != nil {
		log.ErrorC(requestId, err, log.Data{"Details": "Invalid outputUrl"})
		return NilRequest, err
	}
	return FilterRequest{RequestID: requestId, InputURL: input, OutputURL: output, Dimensions: dimensions}, nil
}

func (f *FilterRequest) String() string {
	return fmt.Sprintf(`FilterRequest{RequestID: "%v", InputURL:"%s", OutputURL: "%s", Dimensions: %v}`, f.RequestID, f.InputURL.String(), f.OutputURL.String(), f.Dimensions)
}
