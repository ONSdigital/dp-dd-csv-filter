package filter_test

import (
	"testing"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/ONSdigital/dp-dd-csv-filter/filter"
	"os"
	"bufio"
	"fmt"
)

func TestProcessor(t *testing.T) {

	inputFileLocation := "../sample_csv/Open-Data-for-filter.csv"
	outputFileLocation := "../build/wibble.csv"

	Convey("Given a processor pointing to a local csv file", t, func() {

		var Processor = filter.NewCSVProcessor()

		inputFile := openFile(inputFileLocation, "Error loading input file. Does it exist? ")
		outputFile := createFile(outputFileLocation, "Error creating output file.")

		Convey("When the processor is called with no dimensions to filter \n", func() {
			dimensions := map[string][]string{}
			Processor.Process(bufio.NewReader(inputFile), bufio.NewWriter(outputFile), dimensions)
			So(countLinesInFile(outputFileLocation) == 277, ShouldBeTrue)
		})

		Convey("When the processor is called with a single dimension to filter \n", func() {
			dimensions := map[string][]string{"NACE":{"08 - Other mining and quarrying"}}
			Processor.Process(bufio.NewReader(inputFile), bufio.NewWriter(outputFile), dimensions)
			So(countLinesInFile(outputFileLocation) == 10, ShouldBeTrue)

		})
		Convey("When the processor is called with 2 dimensions to filter \n", func() {
			dimensions := map[string][]string{
				"NACE":{"08 - Other mining and quarrying"},
				"Prodcom Elements":{"Work done"} }
			Processor.Process(bufio.NewReader(inputFile), bufio.NewWriter(outputFile), dimensions)
			So(countLinesInFile(outputFileLocation) == 2, ShouldBeTrue)

		})
		Convey("When the processor is called with 2 dimensions to filter and one of them has multiple acceptable values \n", func() {
			dimensions := map[string][]string{
				"NACE":{"08 - Other mining and quarrying", "1012 - Processing and preserving of poultry meat"},
				"Prodcom Elements":{"Work done", "Waste Products"} }
			Processor.Process(bufio.NewReader(inputFile), bufio.NewWriter(outputFile), dimensions)
			So(countLinesInFile(outputFileLocation) == 5, ShouldBeTrue)

		})

	})

}

func countLinesInFile(fileLocation string)(int) {
	finalFile, err := os.Open(fileLocation)
	if err != nil {
		fmt.Println("Error reading output file", err.Error())
		panic(err)
	}
	scanner := bufio.NewScanner(bufio.NewReader(finalFile))
	counter := 0
	for scanner.Scan() {
		counter++
	}
	fmt.Println("Lines read: %d", counter)
	return counter
}

func openFile(fileLocation string, errorMsg string)(*os.File) {
	file, err := os.Open(fileLocation)
	if err != nil {
		fmt.Println(errorMsg, err.Error())
		panic(err)
	}
	return file
}

func createFile(fileLocation string, errorMsg string)(*os.File) {
	file, err := os.Create(fileLocation)
	if err != nil {
		fmt.Println(errorMsg, err.Error())
		panic(err)
	}
	return file
}