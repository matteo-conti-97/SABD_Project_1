# Hard Disk Failure Data Processing

## How to setup
0) Open a terminal in project root directory which we call $PROJECT_DIR
1) Run command "docker compose up" or "make up" and wait startup of all containers
2) In your browser access to Nifi Web UI at https://localhost:8443/nifi and perform login with the credentials specified in docker-compose.yml
3) Upload Nifi template file available at $PROJECT_DIR/Dockerfiles/nifi/templates/progetto_1.4.xml or Nifi JSON flow file available at $PROJECT_DIR/Dockerfiles/nifi/templates/progetto_1.4.json
4) Start all Nifi processors if you want to ingest data in HDFS with both csv and parquet format or only the processors in one of the two branches if you want only one format
5) To connect HDFS and Spark use port 8020


## How to ingest data
If you want to start only the ingestion containers you can use the “make up-ingestion”

0) Copy the data CSV file in $PROJECT_DIR/data/dataset and make sure it's named raw_data_medium-utv_sorted.csv
1) Do one of this things:
    - Run the python script data_sender.py available at $PROJECT_DIR
    - open a terminal in $PROJECT_DIR/data/dataset and run the command "curl -X POST raw_data_medium-utv_sorted.csv http://localhost:5200/listener
    - Run "make data-sender"

To verify the ingestion result look in HDFS Web UI available at http://localhost:9870 in the browse "filesystem" section or check the progression status of the Nifi processors in Nifi Web UI available at https://localhost:8443/nifi

# How to processing data
If you want to start only the ingestion containers you can use the “make up-processing”.

You can easily run the queries using the MakeFile running the following commands:
    - make query1
    - make query2
    - make query3

# How to see results in MongoDB
Se vuoi vedere i risultati dell'esecuzione delle query puoi usare mongo express andando a connetterti alla Web UI fornita http://localhost:8081 e visualizzare il database "results"



# How to use Grafana
Once you have launched the graphana container with docker compose connect to the local web UI available at http://localhost:3000/ and then log in with the credentials username: “admin” password: “admin”. To view the dashboard simply go to the dashboards panel on the left and select “Results”

![grafana_dashboard](https://github.com/matteo-conti-97/hard_disk_failure_data_processing/assets/30274870/49275a82-ca9d-4a1f-9500-ab5edc1ae8c4)
