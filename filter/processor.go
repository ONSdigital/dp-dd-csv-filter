package filter

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"

	"github.com/ONSdigital/go-ns/log"
	"time"
)

const (
	DIMENSION_START_INDEX = 3
)

// CSVProcessor defines the CSVProcessor interface.
type CSVProcessor interface {
	Process(requestId string, r io.Reader, w io.Writer, dimensions map[string][]string)
}

// Processor implementation of the CSVProcessor interface.
type Processor struct{}

// NewCSVProcessor create a new Processor.
func NewCSVProcessor() *Processor {
	return &Processor{}
}

func getDimensionLocations(row []string) map[string]int {
	result := make(map[string]int)
	for i := DIMENSION_START_INDEX; i < len(row); i = i + 3 {
		dim := strings.TrimSpace(row[i+1])
		result[dim] = i + 2 // value is next field after dim name
	}

	return result
}

func (p *Processor) Process(requestId string, r io.Reader, w io.Writer, dimensions map[string][]string) {
	lineCounter := 0
	linesWritten := 0
	startTime := time.Now()
	defer func() {
		endTime := time.Now()
		log.DebugC(requestId, fmt.Sprintf("Process, duration_ns: %d", endTime.Sub(startTime).Nanoseconds()), log.Data{})
	}()

	csvReader, csvWriter := csv.NewReader(r), csv.NewWriter(w)
	defer csvWriter.Flush()

	dimensionLocations := make(map[string]int)

csvLoop:
	for {
		row, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				fmt.Println("EOF reached, no more records to process", err.Error())
				break csvLoop
			} else {
				fmt.Println("Error occurred and cannot process anymore entry", err.Error())
				panic(err)
			}
		}

		if lineCounter == 0 || len(dimensions) < 1 {
			writeLine(requestId, csvWriter, row)
			linesWritten++
		} else {
			if lineCounter == 1 {
				dimensionLocations = getDimensionLocations(row)
			}
			if allDimensionsMatch(row, dimensions, dimensionLocations) {
				writeLine(requestId, csvWriter, row)
				linesWritten++
			}
		}
		lineCounter++
	}
	log.DebugC(requestId, fmt.Sprintf("Finished processing csv file, filter result: %d of %d rows", linesWritten, lineCounter), nil)
}

func writeLine(requestId string, csvWriter *csv.Writer, row []string) {
	err := csvWriter.Write(row)
	if err != nil {
		log.ErrorC(requestId, err, log.Data{"error": err, "message": "Error occured whilst writing to csv file"})
	}
}

func allDimensionsMatch(row []string, dimensions map[string][]string, dimensionLocations map[string]int) bool {
	for targetDim, targetValues := range dimensions {

		dimLocation := dimensionLocations[targetDim]
		actualValue := row[dimLocation]

		if !singleDimensionMatches(actualValue, targetValues) {
			return false
		}
	}
	return true
}

func singleDimensionMatches(actualValue string, targetValues []string) bool {
	for _, v := range targetValues {
		//fmt.Println("Search for: " + targetValue)
		//fmt.Println("Actual: " + actualValue)
		if v == actualValue {
			return true
		}
	}
	return false
}
