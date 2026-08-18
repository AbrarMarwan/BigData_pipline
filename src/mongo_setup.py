import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) 
from pymongo import MongoClient
from config.settings import MONGO_URI, DB_NAME, COLLECTION_RAW, COLLECTION_VALIDATED, COLLECTION_QUARANTINE

def init_mongo():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    # التأكد من إنشاء المجموعات
    # إنشاء Unique Index على order_id في validated لضمان الـ Idempotency
    db[COLLECTION_VALIDATED].create_index([("order_id", 1)], unique=True)
    
    print("MongoDB collections and indexes initialized.")
    client.close()

if __name__ == "__main__":
    init_mongo()