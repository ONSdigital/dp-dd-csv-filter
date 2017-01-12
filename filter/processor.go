package filter

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"
)

// CSVProcessor defines the CSVProcessor interface.
type CSVProcessor interface {
	Process(r io.Reader, w io.Writer, dimensions map[string][]string)
}

// Processor implementation of the CSVProcessor interface.
type Processor struct{}

// NewCSVProcessor create a new Processor.
func NewCSVProcessor() *Processor {
	return &Processor{}
}

func getDimensionLocations(row []string) map[string]int {
	result := make(map[string]int)
	for i, j := 10, 0; i < len(row); i, j = i+2, j+1 {
		dim := strings.TrimSpace(row[i])
		result[dim] = i + 1 // value is next field after dim name
	}

	return result
}

func (p *Processor) Process(r io.Reader, w io.Writer, dimensions map[string][]string) {

	csvReader, csvWriter := csv.NewReader(r), csv.NewWriter(w)
	defer csvWriter.Flush()

	dimensionLocations := make(map[string]int)

	lineCounter := 0
csvLoop:
	for {
		row, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				fmt.Println("EOF reached, no more records to process", err.Error())
				break csvLoop
			} else {
				fmt.Println("Error occored and cannot process anymore entry", err.Error())
				panic(err)
			}
		}

		if lineCounter == 0 || len(dimensions) < 1 {
			writeLine(csvWriter, row)
		} else {
			if lineCounter == 1 {
				dimensionLocations = getDimensionLocations(row)
				fmt.Printf("%v", dimensionLocations)
			}
			if allDimensionsMatch(row, dimensions, dimensionLocations) {
				writeLine(csvWriter, row)
			}
		}
		lineCounter++
	}
}

func writeLine(csvWriter *csv.Writer, row []string) {
	err := csvWriter.Write(row)
	if err != nil {
		fmt.Println("Error occured whilst writing to csv file", err.Error())
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
