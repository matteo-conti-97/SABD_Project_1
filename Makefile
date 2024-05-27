hello_world:
	docker compose exec spark-master spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 --master spark://spark-master:7077 hello_world.py
query1:
	docker compose exec spark-master spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 --master spark://spark-master:7077 query1.py
query2:
	docker compose exec spark-master spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 --master spark://spark-master:7077 query2.py
query3:
	docker compose exec spark-master spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 --master spark://spark-master:7077 query3.py
up-processing:
	docker compose up -d --build spark-master spark-worker mongo mongo-express namenode datanode1 datanode2 
shut-down:
	docker compose down
