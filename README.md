# Hard Disk Failure Data Processing

## How to setup
0) Open a terminal in project root directory which we call $PROJECT_DIR
1) Run command "docker compose up" and wait startup of all containers
2) In your browser access to Nifi Web UI at https://localhost:8443/nifi and perform login with the credentials specified in docker-compose.yml
3) Upload Nifi template file available at $PROJECT_DIR/Dockerfiles/nifi/templates/progetto_1.4.xml or Nifi JSON flow file available at $PROJECT_DIR/Dockerfiles/nifi/templates/progetto_1.4.json
4) Start all Nifi processors if you want to ingest data in HDFS with both csv and parquet format or only the processors in one of the two branches if you want only one format
5) To connect HDFS and Spark use port 8020


## How to ingest data
0) Copy the data CSV file in $PROJECT_DIR/data/dataset and rename it as disk_data.csv
1) Run the python script data_sender.py available at $PROJECT_DIR or open a terminal in $PROJECT_DIR/data/dataset and run the command "curl -X POST disk_data.csv http://localhost:5200/listener

To verify the ingestion result look in HDFS Web UI available at http://localhost:9870 in the browse "filesystem" section or check the progression status of the Nifi processors in Nifi Web UI available at https://localhost:8443/nifi

# How to use Grafana
Once you have launched the graphana container with docker compose connect to the local web UI available at http://localhost:3000/ and then log in with the credentials username: “admin” password: “admin”. To view the dashboard simply go to the dashboards panel on the left and select “Results”

