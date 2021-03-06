package filter_test

import (
	"bufio"
	"fmt"
	"os"
	"testing"

	"github.com/ONSdigital/dp-dd-csv-filter/filter"
	. "github.com/smartystreets/goconvey/convey"
)

func TestProcessor(t *testing.T) {

	inputFileLocation := "../sample_csv/Open-Data-v3.csv"

	Convey("Given a processor pointing to a local csv file", t, func() {

		var Processor = filter.NewCSVProcessor()

		inputFile := openFile(inputFileLocation, "Error loading input file. Does it exist? ")
		outputFile := createFileInBuildDir("wibble.csv", "Error creating output file.")

		Convey("When the processor is called with no dimensions to filter \n", func() {
			dimensions := map[string][]string{}
			Processor.Process("requestId", bufio.NewReader(inputFile), bufio.NewWriter(outputFile), dimensions)
			So(countLinesInFile(outputFile.Name()) == 277, ShouldBeTrue)
		})

		Convey("When the processor is called with a single dimension to filter \n", func() {
			dimensions := map[string][]string{"NACE": {"CI_0000072"}} // 08 - Other mining and quarrying
			Processor.Process("requestId", bufio.NewReader(inputFile), bufio.NewWriter(outputFile), dimensions)
			So(countLinesInFile(outputFile.Name()) == 10, ShouldBeTrue)

		})
		Convey("When the processor is called with 2 dimensions to filter \n", func() {
			dimensions := map[string][]string{
				"NACE":             {"CI_0000072"}, // 08 - Other mining and quarrying
				"Prodcom Elements": {"CI_0021513"}} // Work done
			Processor.Process("requestId", bufio.NewReader(inputFile), bufio.NewWriter(outputFile), dimensions)
			So(countLinesInFile(outputFile.Name()) == 2, ShouldBeTrue)

		})
		Convey("When the processor is called with 2 dimensions to filter and one of them has multiple acceptable values \n", func() {
			dimensions := map[string][]string{
				"NACE":             {"CI_0000072", "CI_0008197"}, // "08 - Other mining and quarrying", "1012 - Processing and preserving of poultry meat"
				"Prodcom Elements": {"CI_0021513", "CI_0021514"}} // "Work done", "Waste Products"
			Processor.Process("requestId", bufio.NewReader(inputFile), bufio.NewWriter(outputFile), dimensions)
			So(countLinesInFile(outputFile.Name()) == 5, ShouldBeTrue)

		})

	})

}

func countLinesInFile(fileLocation string) int {
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

func openFile(fileLocation string, errorMsg string) *os.File {
	file, err := os.Open(fileLocation)
	if err != nil {
		fmt.Println(errorMsg, err.Error())
		panic(err)
	}
	return file
}

func createFileInBuildDir(fileName string, errorMsg string) *os.File {
	if _, err := os.Stat("../build"); os.IsNotExist(err) {
		os.Mkdir("../build", os.ModePerm)
	}

	file, err := os.Create("../build/" + fileName)

	if err != nil {
		fmt.Println(errorMsg, err.Error())
		panic(err)
	}

	return file
}
