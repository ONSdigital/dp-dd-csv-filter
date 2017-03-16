package ons_aws

import (
	"encoding/json"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestUnmarshal(t *testing.T) {
	input := "\"s3://bucket/file\""

	Convey("Given json representing a valid s3url is unmarshaled", t, func() {

		var s3url S3URL
		json.Unmarshal([]byte(input), &s3url)

		Convey("Then the s3url should have correct bucket and filename", func() {
			So(s3url.GetBucketName(), ShouldEqual, "bucket")
			So(s3url.GetFilePath(), ShouldEqual, "file")
		})

	})
}

func TestUnmarshalInvalid(t *testing.T) {
	input := "\"s3://bucket/\""

	Convey("Given json representing an invalid s3url is unmarshaled", t, func() {

		var s3url S3URL
		err := json.Unmarshal([]byte(input), &s3url)

		Convey("Then the s3url should be NilS3URL", func() {
			So(s3url, ShouldResemble, NilS3URL)
		})
		Convey("and err should not be nil", func() {
			So(err, ShouldNotEqual, nil)
		})

	})
}

func TestS3URLCanBeMarshaledAndUnmarshaled(t *testing.T) {
	var original, _ = NewS3URL("s3://bucket/file")

	Convey("Given a url marshaled to json", t, func() {
		var marshaled, _ = json.Marshal(original)
		Convey("Then the unmarshaled object should resemble the original", func() {
			var unmarshaled S3URL
			err := json.Unmarshal(marshaled, &unmarshaled)
			So(err, ShouldEqual, nil)
			So(unmarshaled, ShouldResemble, original)
		})
	})
}

func TestString(t *testing.T) {
	var original, _ = NewS3URL("s3://bucket/file")

	Convey("String should include the s3 url", t, func() {
		Convey("Then the string should resemble the original", func() {
			So(original.String(), ShouldEqual, "s3://bucket/file")
		})
	})
}

func TestNew(t *testing.T) {

	Convey("Given a valid s3 url", t, func() {

		s3url, err := NewS3URL("s3://bucket/file")

		Convey("Then the s3url should have correct bucket and filename", func() {
			So(s3url.GetBucketName(), ShouldEqual, "bucket")
			So(s3url.GetFilePath(), ShouldEqual, "file")
		})
		Convey("and err should be nil", func() {
			So(err, ShouldEqual, nil)
		})

	})
}

func TestInvalidHost(t *testing.T) {

	Convey("Given url without a host", t, func() {

		s3url, err := NewS3URL("/file")

		Convey("Then the s3url should be NilS3URL", func() {
			So(s3url, ShouldResemble, NilS3URL)
		})
		Convey("and err should not be nil", func() {
			So(err, ShouldNotEqual, nil)
		})

	})
}

func TestInvalidPath(t *testing.T) {

	Convey("Given url without a path", t, func() {

		s3url, err := NewS3URL("s3://host/")

		Convey("Then the s3url should be NilS3URL", func() {
			So(s3url, ShouldResemble, NilS3URL)
		})
		Convey("and err should not be nil", func() {
			So(err, ShouldNotEqual, nil)
		})

	})
}
