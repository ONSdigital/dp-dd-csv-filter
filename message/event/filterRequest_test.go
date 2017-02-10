package event

import (
	"encoding/json"
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const inputBucket = "input-bucket-name"
const inputFile = "input_folder/filter.csv"
const outputBucket = "output-bucket-name"
const outputFile = "output_folder/filter.csv"

var inputUrl = fmt.Sprintf("s3://%s/%s", inputBucket, inputFile)
var outputUrl = fmt.Sprintf("s3://%s/%s", outputBucket, outputFile)

func TestNewFilterRequest(t *testing.T) {

	Convey("Given a call to NewFilterRequest", t, func() {

		var filterRequest, _ = NewFilterRequest(inputUrl, outputUrl, map[string][]string{})

		Convey("Then the inputUrl should have correct bucket and filename", func() {
			So(filterRequest.InputURL.GetBucketName(), ShouldEqual, inputBucket)
			So(filterRequest.InputURL.GetFilePath(), ShouldEqual, inputFile)
		})
		Convey("And the outputUrl should have correct bucket and filename", func() {
			So(filterRequest.OutputURL.GetBucketName(), ShouldEqual, outputBucket)
			So(filterRequest.OutputURL.GetFilePath(), ShouldEqual, outputFile)
		})

	})
}

func TestNewValidatesInputURL(t *testing.T) {
	Convey("Given a call to NewFilterRequest with an invalid input", t, func() {
		var filterRequest, err = NewFilterRequest("invalid url", outputUrl, map[string][]string{})
		Convey("Then returned request is nil and err is not", func() {
			So(err, ShouldNotEqual, nil)
			So(filterRequest, ShouldResemble, NilRequest)
		})
	})
}

func TestNewValidatesOutputURL(t *testing.T) {
	Convey("Given a call to NewFilterRequest with an invalid input", t, func() {
		var filterRequest, err = NewFilterRequest(inputUrl, "invalid url", map[string][]string{})
		Convey("Then returned request is nil and err is not", func() {
			So(err, ShouldNotEqual, nil)
			So(filterRequest, ShouldResemble, NilRequest)
		})
	})
}

func TestFilterRequestCanBeMarshaledAndUnmarshaled(t *testing.T) {
	var filterRequest, _ = NewFilterRequest(inputUrl, outputUrl, map[string][]string{})

	Convey("Given a filterRequest marshaled to json", t, func() {
		var marshaled, _ = json.Marshal(filterRequest)
		Convey("Then the unmarshaled object should resemble the original", func() {
			var unmarshaled FilterRequest
			err := json.Unmarshal(marshaled, &unmarshaled)
			So(err, ShouldEqual, nil)
			So(unmarshaled, ShouldResemble, filterRequest)
		})
	})
}
func TestString(t *testing.T) {
	var filterRequest, _ = NewFilterRequest(inputUrl, outputUrl, map[string][]string{"Foo": {"bar"}})

	Convey("Given a filterRequest", t, func() {
		Convey("Then the String() should resemble the original", func() {
			s := filterRequest.String()
			So(s, ShouldEqual, `FilterRequest{InputURL:"s3://input-bucket-name/input_folder/filter.csv", OutputURL: "s3://output-bucket-name/output_folder/filter.csv", Dimensions: map[Foo:[bar]]}`)
		})
	})
}
