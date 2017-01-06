package filter

import (
	"github.com/Shopify/sarama"
	"io"
	"encoding/csv"
	"fmt"
	"strings"
)

var Producer sarama.AsyncProducer


// CSVProcessor defines the CSVProcessor interface.
type CSVProcessor interface {
	Process(r io.Reader, w io.Writer, dimensions [][]string)
}

// Processor implementation of the CSVProcessor interface.
type Processor struct{}

// NewCSVProcessor create a new Processor.
func NewCSVProcessor() *Processor {
	return &Processor{}
}

func getDimensionLocations(row []string) (map[string]int) {
	result := make (map[string]int)
	for i, j := 10, 0; i < len(row); i, j = i + 2, j + 1 {
		dim := strings.TrimSpace(row[i])
		result[dim] = i
	}

	return result
}

func (p *Processor) Process(r io.Reader, w io.Writer, dimensions [][]string) {

	csvReader := csv.NewReader(r)
	csvWriter := csv.NewWriter(w)

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
			if(lineCounter == 1) {
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

func allDimensionsMatch(row []string, dimensions [][]string, dimensionLocations map[string]int) (bool) {
	for i := range dimensions {
		targetDim := dimensions[i][0]
		dimLocation := dimensionLocations[targetDim] + 1 // to get the value and not the dim name

		actualValue := row[dimLocation]
		expectedValue := dimensions[i][1]

		//fmt.Println("Actual: " + actualValue)
		//fmt.Println("Search for: " + expectedValue)

		if actualValue != expectedValue {
			return false
		}
	}
	return true
}