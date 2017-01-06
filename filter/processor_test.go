package filter_test

import (
	"testing"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/ONSdigital/dp-dd-csv-filter/filter"
	"os"
	"bufio"
	"fmt"
	"path/filepath"
)

func TestProcessor(t *testing.T) {

	// todo - how to handle test resources in GO
	inputFileLocation := "/Users/allen/projects/ons/src/github.com/ONSdigital/dp-dd-csv-filter/sample_csv/Open-Data-new-format.csv"
	outputFileLocation := "/Users/allen/projects/ons/src/github.com/ONSdigital/dp-dd-csv-filter/build/wibble.csv"

	Convey("Given a processor pointing to a local csv file", t, func() {

		var Processor = filter.NewCSVProcessor()

		dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
		if err != nil {
			panic(err)
		}
		fmt.Println("\n\nCurrent working dir: " + dir)

		file, err := os.Open(inputFileLocation)
		if err != nil {
			fmt.Println("Error loading input file. Does it exist?", err.Error())
			panic(err)
		}

		outFile, err := os.Create(outputFileLocation)
		if err != nil {
			fmt.Println("Error creating output file.", err.Error())
			panic(err)
		}


		Convey("When the processor is called with no dimensions to filter \n", func() {
			dimensions := [][]string{}
			Processor.Process(bufio.NewReader(file), bufio.NewWriter(outFile), dimensions)
			So(countLinesInFile(outputFileLocation) == 278, ShouldBeTrue)
		})

		Convey("When the processor is called with a single dimension to filter \n", func() {
			dimensions := [][]string{{"NACE", "08 - Other mining and quarrying"}}
			Processor.Process(bufio.NewReader(file), bufio.NewWriter(outFile), dimensions)
			So(countLinesInFile(outputFileLocation) == 10, ShouldBeTrue)

		})

		Convey("When the processor is called with 2 dimensions to filter \n", func() {
			dimensions := [][]string{
				{"NACE", "08 - Other mining and quarrying"},
				{"Prodcom Elements", "Work done"}}
			Processor.Process(bufio.NewReader(file), bufio.NewWriter(outFile), dimensions)
			So(countLinesInFile(outputFileLocation) == 2, ShouldBeTrue)

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
