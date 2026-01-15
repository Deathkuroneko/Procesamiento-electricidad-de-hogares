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


TXT (Kaggle)
   ↓
Preprocesamiento (Local / Colab)
   ↓
CSV limpio
   ↓
Kafka Producer
   ↓
Kafka Topic
   ↓
Spark Streaming
   ↓
HDFS (histórico)
   ↓
MongoDB (resultados)


👉 Paso 1 (hecho): TXT → CSV
👉 Paso 2 (siguiente): Kafka Producer leyendo CSV
👉 Paso 3: Spark Streaming
👉 Paso 4: Guardar en HDFS y MongoDB

bigdata-kappa-energy/
│
├── data/
│   ├── raw/
│   └── processed/
│
├── kafka/
│   └── producer.py
│
├── spark/
│   ├── streaming.py
│   └── batch.py
│
├── hdfs/
│   └── README.md
│
├── mongodb/
│   └── README.md
│
├── docker-compose.yml
└── README.md

Paso 3
Kafka (única fuente)
   ↓
Spark Structured Streaming
   ↓            ↓
 MongoDB        HDFS
 (speed)     (batch/histórico)

spark/streaming.py

Descargar el JAR:

mongo-spark-connector_2.13-10.2.0.jar


Colocarlo en:

spark/jars/

Y en docker-compose.yml (spark + worker):

environment:
  - SPARK_EXTRA_CLASSPATH=/opt/spark/jars/*

  3️⃣ Copiar el job al contenedor Spark

Desde la raíz del proyecto:

docker cp spark/streaming.py spark:/opt/spark/jobs/streaming.py


Verifica:

docker exec -it spark ls /opt/spark/jobs

4️⃣ Ejecutar Spark Streaming
docker exec -it spark spark-submit \
--master spark://spark:7077 \
/opt/spark/jobs/streaming.py

5️⃣ Prueba REAL (pipeline completo)

1️⃣ Arranca Spark Streaming
2️⃣ En otra terminal ejecuta el producer.py
3️⃣ Observa:

docker logs spark --tail 30


4️⃣ Revisa HDFS:

docker exec -it hdfs-namenode hdfs dfs -ls /energy/raw


5️⃣ Revisa Mongo:

docker exec -it mongodb mongosh
use energy
db.streaming.find().limit(5)

| Principio Kappa | Evidencia                  |
| --------------- | -------------------------- |
| Fuente única    | Kafka                      |
| Streaming       | Spark Structured Streaming |
| Histórico       | HDFS                       |
| Reprocesar      | Spark lee HDFS             |
| Simplicidad     | Un pipeline                |
