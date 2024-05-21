hello_world:
	docker compose exec spark-master spark-submit --master spark://spark-master:7077 hello_world.py
