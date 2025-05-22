from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import StructType, StringType, FloatType

# Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("KafkaSensorConsumer") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema sesuai struktur JSON yang dikirim Kafka
schema_suhu = StructType() \
    .add("gudang_id", StringType()) \
    .add("suhu", FloatType())

schema_kelembaban = StructType() \
    .add("gudang_id", StringType()) \
    .add("kelembaban", FloatType())

# Fungsi bantu untuk proses stream
def consume_kafka_topic(topic_name, field_name, threshold, warning_message):
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic_name) \
        .load()

    json_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema_suhu if field_name == "suhu" else schema_kelembaban).alias("data")) \
        .select("data.*")

    filtered_df = json_df.filter(col(field_name) > threshold) \
        .withColumn("tipe_peringatan", lit(warning_message)) \
        .select("tipe_peringatan", "gudang_id", col(field_name).alias("nilai"))

    query = filtered_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    return query

# Konsumsi suhu > 80
q1 = consume_kafka_topic("sensor-suhu-gudang", "suhu", 80, "[Peringatan Suhu Tinggi]")

# Konsumsi kelembaban > 70
q2 = consume_kafka_topic("sensor-kelembaban-gudang", "kelembaban", 70, "[Peringatan Kelembaban Tinggi]")

# Tunggu semua query selesai
q1.awaitTermination()
q2.awaitTermination()

