# Hard Disk Failure Data Processing
This project presents the implementation of a big data batch processing pipeline aimed at analyzing hard disk failures. The pipeline, containerized and managed through Docker Compose, uses Apache NiFi for data ingestion, HDFS for storage, Spark for processing, MongoDB for analytical storage, and Grafana for results visualization.

![pipeline](https://github.com/matteo-conti-97/hard_disk_failure_data_processing/assets/30274870/df1bf836-d464-4160-b0c7-305afe0fce55)

The analyzed dataset, a shortened version of the Grand Challenge dataset from the ACM DEBS 2024 conference, contains information on measurement date, hard drive identifier, model, power-on hours, and failures. Three queries were run to analyze the data and compare the performance of CSV and Parquet file formats. 

The queries are the following:
1) For each day, for each vault (refer to the vault id field), calculate the total number of failures. Determine the list of vaults that experienced exactly 4, 3 and 2 failures
2) Calculate the ranking of the 10 hard disk drive models that have suffered the most failures. The ranking must report the hard disk model and the total number of failures suffered by hard disks of that specific model. Next, calculate a second ranking of the 10 vaults that experienced the most failures. For each vault, report the number of failures and the list (without repetition) of models of hark disks subject to at least one failure.
3) Calculate the minimum, 25th, 50th, 75th percentile, and maximum operating hours (field s9 power on hours) of hark disks that have experienced failures and hard disks that have not have experienced failures. Pay attention, the s9 power on hours field reports a cumulative, so the statistics required by the query must refer to the last useful day of detection for each specific hard disk (consider the use of the serial number field). In the output also indicate the total number of events used to calculate the statistics.

For more details on queries read [this](https://github.com/matteo-conti-97/hard_disk_failure_data_processing/blob/main/Traccia.pdf).

## What do you need
To use the project you need the following things:
- Docker
- Dataset from the ACM DEBS 2024 conference
- Python

## How to setup
0) Open a terminal in project root directory which we call $PROJECT_DIR
1) Run command "docker compose --profile all up" or "make up" and wait the startup of all containers
2) From browser access to Nifi web UI at https://localhost:8443/nifi and perform login with the credentials specified in docker-compose.yml
3) Upload Nifi template file available at $PROJECT_DIR/Dockerfiles/nifi/templates/progetto_1.4.xml, to do so right mouse click on a blank space of Nifi UI and select "upload template" then drag and drop from the template icon in the UI to use it.
3) Start all the Nifi controller services needed for the processors, to do so right mouse click on a blank space of Nifi UI and select "enable all controller services" then wait for the services to start.
4) Start all Nifi processors if you want to ingest data in HDFS with both CSV and Parquet formats or only the processors in one of the two branches if you want only one format, to do so right mouse click on a processor and select "start"
5) To verify that HDFS is up access the web UI at  http://localhost:9870
6) To verify that Spark is up access the web UI at http://localhost:8080 and verify that all workers are connected
7) To verify that MongoDB is up access the Mongo Express web UI at http://localhost:8081
8) To verify that Grafana is up access the web UI at http://localhost:3000/login


## How to ingest data
If you want to start only the ingestion containers you can use the “make up-ingestion”

0) Copy the data CSV file in $PROJECT_DIR/data/dataset and make sure it's named raw_data_medium-utv_sorted.csv
1) Do one of this things:
    - Run the python script data_sender.py available at $PROJECT_DIR
    - Open a terminal in $PROJECT_DIR/data/dataset and run the command "curl -X POST raw_data_medium-utv_sorted.csv http://localhost:5200/listener
    - Open a terminal in $PROJECT_DIR and run "make data-sender" 

To check the progression status of the Nifi processors in Nifi Web UI available at https://localhost:8443/nifi.
To verify the ingestion result look in HDFS Web UI available at http://localhost:9870 in the browse "filesystem" section, if you activated both Nifi branches there should be 2 files disk_data_filtered.parquet and disk_data_filtered.csv

## How to processing data
If you want to start only the ingestion containers you can use the “make up-processing”.

You can easily run the queries using the Makefile in $PROJECT_DIR running the following commands:
- make query1
- make query2
- make query3

## How to see results in MongoDB
If you want to see the results of query execution you can use mongo express by going to connect to the Web UI provided http://localhost:8081 and view the database “results”

![mongo](https://github.com/matteo-conti-97/hard_disk_failure_data_processing/assets/30274870/9ef5788e-40ca-452c-a832-78ffdbd474d2)



## How to use Grafana
Once you have launched the graphana container with docker compose connect to the local web UI available at http://localhost:3000/ and then log in with the credentials username: “admin” password: “admin”. To view the dashboard simply go to the dashboards panel on the left and select “Results”

![grafana_dashboard](https://github.com/matteo-conti-97/hard_disk_failure_data_processing/assets/30274870/49275a82-ca9d-4a1f-9500-ab5edc1ae8c4)

## More details
A full report on the work done is available [here](https://github.com/matteo-conti-97/hard_disk_failure_data_processing/blob/main/report/report.pdf)

