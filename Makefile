query1_csv:
	docker compose exec spark-master spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 --master spark://spark-master:7077 query1.py CSV
query1_parquet:
	docker compose exec spark-master spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 --master spark://spark-master:7077 query1.py PARQUET
query2:
	docker compose exec spark-master spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 --master spark://spark-master:7077 query2.py
query3:
	docker compose exec spark-master spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 --master spark://spark-master:7077 query3.py
up-processing:
	docker compose --profile processing up -d --build 
up-ingestion:
	docker compose --profile ingestion up -d --build
up:
	docker compose --profile all up -d --build
data-sender:
	python data_sender.py
shut-down:
	docker compose down
