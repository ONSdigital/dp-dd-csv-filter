package event

import (
	"fmt"

	"github.com/ONSdigital/dp-dd-csv-filter/ons_aws"
)

type TransformRequest struct {
	InputURL  ons_aws.S3URL `json:"inputUrl"`
	OutputURL ons_aws.S3URL `json:"outputUrl"`
	RequestID string    `json:"requestId"`
}

// NewTransformRequest creates a new TranformRequest object.
func NewTransformRequest(inputUrl ons_aws.S3URL, outputUrl ons_aws.S3URL, requestId string) TransformRequest {
	return TransformRequest{InputURL: inputUrl, OutputURL: outputUrl, RequestID: requestId}
}

func (f *TransformRequest) String() string {
	return fmt.Sprintf(`TransformRequest{RequestID: "%v", InputURL:"%s", OutputURL: "%s"}`, f.RequestID, f.InputURL.String(), f.OutputURL.String())
}
