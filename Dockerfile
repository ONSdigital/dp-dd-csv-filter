FROM onsdigital/dp-go

WORKDIR /app/

COPY ./build/dp-dd-csv-filter .

ENTRYPOINT ./dp-dd-csv-filter
