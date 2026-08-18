import os

# MongoDB Configurations
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = "ecommerce_pipeline"
COLLECTION_RAW = "orders_raw"
COLLECTION_VALIDATED = "orders_validated"
COLLECTION_QUARANTINE = "orders_quarantine"

# Pipeline Configurations
SMALL_FILE_THRESHOLD_MB = 10  # الحد الفاصل للمحرك
BATCH_SIZE = 1000              # حجم دفعة Python Batch