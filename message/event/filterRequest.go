package event

import (
	"fmt"

	"github.com/ONSdigital/dp-dd-csv-filter/aws"
	"github.com/ONSdigital/go-ns/log"
)

type FilterRequest struct {
	RequestID string               `json:"requestId"`
	InputURL   aws.S3URL           `json:"inputUrl"`
	OutputURL  aws.S3URL           `json:"outputUrl"`
	Dimensions map[string][]string `json:"dimensions"`
}

var NilRequest = FilterRequest{}

// todo: include RequestID - see #528
func NewFilterRequest(inputUrl string, outputUrl string, dimensions map[string][]string) (FilterRequest, error) {
	var input, output aws.S3URL
	var err error
	if input, err = aws.NewS3URL(inputUrl); err != nil {
		log.Error(err, log.Data{"Details": "Invalid inputUrl"})
		return NilRequest, err
	}
	if output, err = aws.NewS3URL(outputUrl); err != nil {
		log.Error(err, log.Data{"Details": "Invalid outputUrl"})
		return NilRequest, err
	}
	return FilterRequest{InputURL: input, OutputURL: output, Dimensions: dimensions}, nil
}

func (f *FilterRequest) String() string {
	return fmt.Sprintf(`FilterRequest{RequestID: "%v", InputURL:"%s", OutputURL: "%s", Dimensions: %v}`, f.RequestID, f.InputURL.String(), f.OutputURL.String(), f.Dimensions)
}
