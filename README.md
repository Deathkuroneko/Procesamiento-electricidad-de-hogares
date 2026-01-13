# Big Data – Arquitectura Kappa
Análisis de consumo energético usando Kafka, Spark, HDFS y MongoDB.

Arquitectura:
- Ingesta: Kafka
- Procesamiento: Spark (Streaming + Batch)
- Almacenamiento: HDFS + MongoDB
- Visualización: Superset

Proyecto académico – Taller I

Inicializar docker:
docker compose down
docker compose up -d

Pruebas:
Loger Master: docker logs spark --tail 30
    Resultadosimilar : Starting Spark master
                        SparkUI available at http://spark:8080


docker logs spark-worker --tail 30: Successfully registered with master spark://spark:7077


http://localhost:8080 - Spark Master
 

 - CREACION DE TOPIC:

 docker exec -it kafka kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic energy-consumption

    verificacion:
        docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

- INSTALAR PYTHON Y PANDAS
    python -m pip install kafka-python pandas
    python -m pip install kafka-python

- Correr Producer.py
    - cd kafka
    - python producer.py

- VER TRASNMISION DE DATOS:
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic energy-consumption --from-beginning
- EJECUTAR EL JOB EN SPARK
docker exec -it spark spark-submit --master spark://spark-master:7077 /opt/spark/jobs/energy_stream.py