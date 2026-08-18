import sys
import os
import time
from datetime import datetime

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

if os.path.exists("C:\\hadoop"):
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["hadoop.home.dir"] = "C:\\hadoop"
    os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import MONGO_URI, DB_NAME, COLLECTION_RAW

def get_spark_session():
    jvm_flags = (
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )

    return SparkSession.builder \
        .appName("HybridDataPipeline_SparkEngine") \
        .master("local[4]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.extraJavaOptions", jvm_flags) \
        .config("spark.executor.extraJavaOptions", jvm_flags) \
        .config("spark.python.worker.reuse", "true") \
        .getOrCreate()

def run_spark_loader(file_path, run_id):
    spark = get_spark_session()
    start_time = time.time()

    columns = [
        "order_id", "order_date", "status", "customer_id", "customer_name",
        "customer_phone", "customer_email", "city", "district", "delivery_type",
        "delivery_cost", "payment_method", "payment_status", "payment_amount",
        "currency", "total_amount", "items_json"
    ]
    schema = StructType([StructField(c, StringType(), True) for c in columns])

    print(f"\n[PySpark Engine] Reading large file: {file_path}")
    
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .schema(schema) \
        .load(file_path)

    df_partitioned = df.repartition(4)
    input_partitions = df_partitioned.rdd.getNumPartitions()

    file_name = os.path.basename(file_path)
    current_time_str = datetime.utcnow().isoformat()

    def insert_partition_to_raw(partition_iter):
        from pymongo import MongoClient
        
        client = MongoClient(MONGO_URI)
        raw_col = client[DB_NAME][COLLECTION_RAW]
        
        batch = []
        count = 0
        
        for row in partition_iter:
            try:
                row_dict = row.asDict()
                clean_dict = {str(k).replace('\ufeff', '').strip(): (str(v) if v is not None else "") for k, v in row_dict.items() if k}
                
                batch.append({
                    "run_id": run_id,
                    "file_source": file_name,
                    "at_ingested": current_time_str,
                    "engine_used": "pyspark",
                    "record_raw": clean_dict
                })
                count += 1
                
                if len(batch) >= 5000:
                    raw_col.insert_many(batch, ordered=False)
                    batch = []
            except Exception:
                continue

        if batch:
            raw_col.insert_many(batch, ordered=False)

        client.close()
        yield count

    print("[PySpark Engine] Ingesting partitions into orders_raw in parallel...")
    counts = df_partitioned.rdd.mapPartitions(insert_partition_to_raw).collect()

    total_rows = sum(counts)
    total_time = time.time() - start_time
    avg_throughput = total_rows / total_time if total_time > 0 else 0

    print(f"\n[PySpark Engine] Ingestion complete: {total_rows} rows loaded in {total_time:.2f}s | Throughput: {avg_throughput:.1f} rows/s")

    return {
        "engine": "pyspark",
        "loaded_raw": total_rows,
        "seconds_elapsed": total_time,
        "throughput": avg_throughput,
        "partitions": input_partitions
    }