package ons_aws

import (
	"io"
	"github.com/ONSdigital/dp-dd-csv-filter/config"
	"github.com/ONSdigital/go-ns/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"time"
	"fmt"
)

// AWSClient interface defining the AWS client.
type AWSService interface {
	// GetFile get the requested file from AWS. The caller is responsible for closing the reader.
	GetCSV(requestID string, s3url S3URL) (io.ReadCloser, error)
	SaveFile(requestID string, reader io.Reader, s3url S3URL) error
}

// Client AWS client implementation.
type Service struct{}

// NewClient create new AWSClient.
func NewService() AWSService {
	return &Service{}
}

func (cli *Service) SaveFile(requestID string, reader io.Reader, s3url S3URL) error {

	startTime := time.Now()
	defer func() {
		endTime := time.Now()
		log.DebugC(requestID, fmt.Sprintf("SaveFile, duration_ns: %d", endTime.Sub(startTime).Nanoseconds()), log.Data{})
	}()

	uploader := s3manager.NewUploader(session.New(&aws.Config{Region: aws.String(config.AWSRegion)}))

	result, err := uploader.Upload(&s3manager.UploadInput{
		Body:   reader,
		Bucket: aws.String(s3url.GetBucketName()),
		Key:    aws.String(s3url.GetFilePath()),
	})

	if err != nil {
		log.Error(err, log.Data{"message": "Failed to upload"})
		return err
	}

	log.Debug("Upload successful", log.Data{
		"uploadLocation": result.Location,
	})

	return nil
}

// GetFile get the requested file from AWS. The caller is responsible for closing the reader.
func (cli *Service) GetCSV(requestID string, s3url S3URL) (io.ReadCloser, error) {
	startTime := time.Now()
	defer func() {
		endTime := time.Now()
		log.DebugC(requestID, fmt.Sprintf("GetCSV, duration_ns: %d", endTime.Sub(startTime).Nanoseconds()), log.Data{})
	}()

	session, err := session.NewSession(&aws.Config{
		Region: aws.String(config.AWSRegion),
	})

	if err != nil {
		log.ErrorC(requestID, err, nil)
		return nil, err
	}

	s3Service := s3.New(session)
	request := &s3.GetObjectInput{}
	request.SetBucket(s3url.GetBucketName())
	request.SetKey(s3url.GetFilePath())

	log.Debug("Requesting .csv file from AWS S3 bucket", log.Data{
		"S3BucketName": request.Bucket,
		"key":          request.Key,
	})
	result, err := s3Service.GetObject(request)

	if err != nil {
		log.ErrorC(requestID, err, log.Data{"request": request})
		return nil, err
	}

	return result.Body, nil
}
