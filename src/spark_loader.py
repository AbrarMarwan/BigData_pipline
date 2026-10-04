import sys
import os
import time
from datetime import datetime

# ============================================================
# Python / Hadoop configuration
# ============================================================

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

if os.path.exists(r"C:\hadoop"):
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["hadoop.home.dir"] = r"C:\hadoop"
    os.environ["PATH"] = (
        r"C:\hadoop\bin;" + os.environ.get("PATH", "")
    )

# ============================================================
# Spark imports
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    monotonically_increasing_id,
    lit,
    col,
    struct,
    spark_partition_id
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

# ============================================================
# Project imports
# ============================================================

sys.path.append(
    os.path.dirname(
        os.path.dirname(
            os.path.abspath(__file__)
        )
    )
)

from config.settings import (
    MONGO_URI,
    DB_NAME,
    COLLECTION_RAW
)

from src.monitor import SystemMonitor


# ============================================================
# MongoDB Spark Connector
# ============================================================

MONGO_SPARK_CONNECTOR = (
    "org.mongodb.spark:mongo-spark-connector_2.12:10.7.0"
)


# ============================================================
# Spark Session
# ============================================================

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

    spark = (
        SparkSession.builder
        .appName("HybridDataPipeline_SparkEngine")

        # Important:
        # keep concurrency reasonable for 16 GB RAM
        .master("local[2]")

        # MongoDB Spark Connector 10.7.0
        .config(
            "spark.jars.packages",
            MONGO_SPARK_CONNECTOR
        )

        # Memory
        .config(
            "spark.driver.memory",
            "4g"
        )
        .config(
            "spark.executor.memory",
            "4g"
        )

        # JVM compatibility
        .config(
            "spark.driver.extraJavaOptions",
            jvm_flags
        )
        .config(
            "spark.executor.extraJavaOptions",
            jvm_flags
        )

        # Python
        .config(
            "spark.pyspark.python",
            sys.executable
        )
        .config(
            "spark.pyspark.driver.python",
            sys.executable
        )

        # Worker stability
        .config(
            "spark.python.worker.reuse",
            "false"
        )

        # Mongo defaults
        .config(
            "spark.mongodb.write.connection.uri",
            MONGO_URI
        )
        .config(
            "spark.mongodb.write.database",
            DB_NAME
        )
        .config(
            "spark.mongodb.write.collection",
            COLLECTION_RAW
        )

        # Do NOT create a huge shuffle
        .config(
            "spark.sql.shuffle.partitions",
            "8"
        )

        .getOrCreate()
    )

    return spark


# ============================================================
# Main PySpark Loader
# ============================================================

def run_spark_loader(file_path, run_id):

    monitor = SystemMonitor(interval=4)
    monitor.start()

    spark = get_spark_session()

    start_time = time.time()

    file_name = os.path.basename(file_path)

    current_time_str = datetime.utcnow().isoformat()

    total_rows = 0
    input_partitions = 0

    # ========================================================
    # Fixed schema
    # ========================================================

    columns = [
        "order_id",
        "order_date",
        "status",
        "customer_id",
        "customer_name",
        "customer_phone",
        "customer_email",
        "city",
        "district",
        "delivery_type",
        "delivery_cost",
        "payment_method",
        "payment_status",
        "payment_amount",
        "currency",
        "total_amount",
        "items_json"
    ]

    # ALL RAW FIELDS ARE STRING
    # This is intentional for ELT/raw ingestion.

    schema = StructType([
        StructField(
            column,
            StringType(),
            True
        )
        for column in columns
    ])

    try:

        # ====================================================
        # 1. READ CSV
        # ====================================================

        print()
        print("=" * 70)
        print("[PySpark Engine] Reading large dataset")
        print("=" * 70)

        print(f"File: {file_path}")

        df = (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("encoding", "UTF-8")
            .option("mode", "PERMISSIVE")
            .option("quote", "\"")
            .option("escape", "\"")
            .schema(schema)
            .load(file_path)
        )

        # ====================================================
        # 2. INPUT PARTITIONS
        # ====================================================

        input_partitions = df.rdd.getNumPartitions()

        print(
            f"[PySpark Engine] Input partitions: "
            f"{input_partitions}"
        )

        # ====================================================
        # 3. BUILD DOCUMENTS
        # ====================================================
        #
        # IMPORTANT:
        #
        # We DO NOT use:
        #
        # groupBy()
        # join()
        # Window()
        # row_number()
        #
        # because these cause a huge shuffle on the 12.65 GB
        # dataset.
        #
        # monotonically_increasing_id() is generated by Spark
        # without a Python UDF and without a global shuffle.
        #
        # It gives a unique Long ID.
        #
        # ====================================================

        print(
            "[PySpark Engine] Building raw documents..."
        )

        documents_df = (
            df

            # Unique source identifier generated by Spark.
            .withColumn(
                "source_row_number",
                (
                    monotonically_increasing_id()
                    + lit(1)
                ).cast("long")
            )

            .withColumn(
                "run_id",
                lit(run_id)
            )

            .withColumn(
                "source_file",
                lit(file_name)
            )

            .withColumn(
                "ingested_at",
                lit(current_time_str)
            )

            .withColumn(
                "engine_used",
                lit("pyspark")
            )

            .withColumn(
                "raw_record",
                struct(
                    *[
                        col(column).alias(column)
                        for column in columns
                    ]
                )
            )

            .select(
                "run_id",
                "source_file",
                "source_row_number",
                "ingested_at",
                "engine_used",
                "raw_record"
            )
        )

        # ====================================================
        # 4. WRITE DIRECTLY TO MONGODB
        # ====================================================

        print()
        print(
            "[PySpark Engine] Writing raw records to MongoDB..."
        )

        print(
            "[PySpark Engine] MongoDB Spark Connector: 10.7.0"
        )

        write_start = time.time()

        (
            documents_df.write
            .format("mongodb")
            .mode("append")

            .option(
                "connection.uri",
                MONGO_URI
            )

            .option(
                "database",
                DB_NAME
            )

            .option(
                "collection",
                COLLECTION_RAW
            )

            .option(
                "maxBatchSize",
                "2000"
            )

            .save()
        )

        write_duration = time.time() - write_start

        # ====================================================
        # 5. COUNT AFTER SUCCESSFUL WRITE
        # ====================================================

        #
        # We only count AFTER the Mongo write.
        #
        # This avoids doing an expensive count before ingestion.
        #

        total_rows = documents_df.count()

        print()
        print(
            "[PySpark Engine] MongoDB write completed successfully."
        )

        print(
            f"[PySpark Engine] Write time: "
            f"{write_duration:.2f} seconds"
        )

    finally:

        monitor.stop()

        spark.stop()

    # ========================================================
    # 6. METRICS
    # ========================================================

    total_time = time.time() - start_time

    throughput = (
        total_rows / total_time
        if total_time > 0
        else 0
    )

    print()
    print("=" * 70)
    print("[PySpark Engine] INGESTION COMPLETE")
    print("=" * 70)

    print(
        f"Rows loaded      : {total_rows:,}"
    )

    print(
        f"Input partitions : {input_partitions}"
    )

    print(
        f"Total time       : {total_time:.2f} seconds"
    )

    print(
        f"Throughput       : {throughput:,.1f} rows/sec"
    )

    print("=" * 70)
    print()

    return {
        "engine": "pyspark",
        "loaded_raw": total_rows,
        "seconds_elapsed": total_time,
        "throughput": throughput,
        "partitions": input_partitions
    }