hello_world:
	docker compose exec spark-master spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 --master spark://spark-master:7077 hello_world.py