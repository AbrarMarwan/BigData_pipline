import os
from dotenv import load_dotenv

load_dotenv()

# MongoDB Configurations
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "midterm_bigdata_db")

COLLECTION_RAW = "orders_raw"
COLLECTION_VALIDATED = "orders_validated"
COLLECTION_QUARANTINE = "orders_quarantine"

# Materialized View Collections
MV_DAILY_SALES = "daily_sales_summary"
MV_TOP_PRODUCTS = "top_products_summary"
MV_METADATA = "mv_metadata"
COLLECTION_JOB_RUNS = "job_runs"

# Routing Threshold
# الملفات الأصغر من أو تساوي 200MB تعمل بـ Python Batch لتجنب Overhead تهيئة Spark
# الملفات الأكبر تنتقل إلى Apache Spark للمعالجة المتوازية الموزعة
SMALL_FILE_THRESHOLD_MB = float(os.getenv("SMALL_FILE_THRESHOLD_MB", 200.0))

# Batch Configurations
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 5000))
ELT_CHUNK_SIZE = int(os.getenv("ELT_CHUNK_SIZE", 5000))

# API Server Configurations
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", 8000))