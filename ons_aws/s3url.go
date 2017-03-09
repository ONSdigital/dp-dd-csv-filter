package ons_aws

import (
	"fmt"
	"net/url"
	"strings"
)

type S3URL struct {
	URL *url.URL
}

var NilS3URL = S3URL{nil}

func NewS3URL(s string) (S3URL, error) {
	s3, err := parseUrl(s)
	if err != nil {
		return NilS3URL, err
	}
	return S3URL{s3}, nil
}

func (s *S3URL) UnmarshalJSON(b []byte) (err error) {
	url, err := parseUrl(strings.Trim(string(b), "\"'"))
	if err != nil {
		return err
	}
	s.URL = url
	return nil
}

func (s S3URL) MarshalJSON() ([]byte, error) {
	if s.URL == nil {
		return []byte("null"), nil
	}
	return []byte("\"" + s.URL.String() + "\""), nil
}

func parseUrl(s string) (*url.URL, error) {
	var u *url.URL
	var err error
	if u, err = url.Parse(s); err != nil {
		return nil, err
	}
	if (len(u.Host)) < 1 {
		return nil, fmt.Errorf("URL '%s' does not contain a Bucket", s)
	}
	if (len(strings.TrimLeft(u.Path, "/"))) < 1 {
		return nil, fmt.Errorf("URL '%s' does not contain a FilePath", s)
	}
	return u, nil
}

func (s *S3URL) GetBucketName() string {
	return s.URL.Host
}

func (s *S3URL) GetFilePath() string {
	return strings.TrimPrefix(s.URL.Path, "/")
}

func (s *S3URL) String() string {
	var u url.URL = *s.URL
	return u.String()
}
