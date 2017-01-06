build:
	go build -o build/dp-csv-filter

debug: build
	HUMAN_LOG=1 ./build/dp-csv-filter

.PHONY: build debug
