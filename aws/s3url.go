package aws

import (
	"fmt"
	"github.com/ONSdigital/go-ns/log"
	"net/url"
	"strings"
)

type S3URLType struct {
	URL *url.URL
}

var NilS3URL = S3URLType{nil}

func NewS3URL(s string) (S3URLType, error) {
	s3, err := parseUrl(s)
	if err != nil {
		return NilS3URL, err
	}
	return S3URLType{s3}, nil
}

func (x *S3URLType) UnmarshalJSON(b []byte) (err error) {
	url, err := parseUrl(strings.Trim(string(b), "\"'"))
	if err != nil {
		log.Error(err, log.Data{"Details": "Failed to unmarshal value to S3URLType"})
		return err
	}
	x.URL = url
	return nil
}

func parseUrl(s string) (*url.URL, error) {
	var u *url.URL
	var err error
	if u, err = url.Parse(s); err != nil {
		return nil, err
	}
	if (len(u.Host)) < 1 {
		return nil, fmt.Errorf("URL %v does not contain a Bucket", u)
	}
	if (len(strings.TrimLeft(u.Path, "/"))) < 1 {
		return nil, fmt.Errorf("URL %v does not contain a FilePath", u)
	}
	return u, nil
}

func (s *S3URLType) GetBucketName() string {
	return s.URL.Host
}

func (s *S3URLType) GetFilePath() string {
	return strings.TrimPrefix(s.URL.Path, "/")
}
