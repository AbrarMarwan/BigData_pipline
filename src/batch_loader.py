import sys
import os
import csv
import time
from datetime import datetime
from pymongo import MongoClient

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import MONGO_URI, DB_NAME, COLLECTION_RAW, BATCH_SIZE

def run_batch_loader(file_path, run_id):
    """
    تحميل الملفات بدفعات مع دعم ميزة الاستئناف التلقائي (Checkpoint Resume)
    """
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    raw_col = db[COLLECTION_RAW]

    file_name = os.path.basename(file_path)

    # 1. الاستعلام عن آخر صف تمت معالجته لنفس الملف (Watermark / Checkpoint)
    last_record = raw_col.find_one(
        {"file_source": file_name},
        sort=[("number_row_source", -1)]
    )
    last_processed_row = last_record.get("number_row_source", 0) if last_record else 0

    if last_processed_row > 0:
        print(f"🔄 Checkpoint detected: Resuming from row #{last_processed_row + 1} (Skipping first {last_processed_row} rows)...")

    start_time = time.time()
    batch = []
    batch_index = 1
    total_loaded = 0

    with open(file_path, mode='r', encoding='utf-8-sig', errors='ignore') as infile:
        reader = csv.DictReader(infile)
        
        for row_num, row in enumerate(reader, start=1):
            # تجاوز السجلات التي سبق تحميلها بنجاح
            if row_num <= last_processed_row:
                continue

            raw_document = {
                "run_id": run_id,
                "file_source": file_name,
                "number_row_source": row_num,
                "at_ingested": datetime.utcnow().isoformat(),
                "engine_used": "python_batch",
                "record_raw": row
            }
            batch.append(raw_document)

            if len(batch) >= BATCH_SIZE:
                b_start = time.time()
                try:
                    raw_col.insert_many(batch, ordered=False)
                    b_duration = time.time() - b_start
                    total_loaded += len(batch)
                    rate = len(batch) / b_duration if b_duration > 0 else 0
                    print(f"  -> Batch #{batch_index} inserted: {len(batch)} records | Rate: {rate:.1f} rec/s")
                except Exception as e:
                    print(f"  [!] Error in batch #{batch_index}: {e}")
                batch = []
                batch_index += 1

        if batch:
            try:
                raw_col.insert_many(batch, ordered=False)
                total_loaded += len(batch)
                print(f"  -> Final Batch inserted: {len(batch)} records")
            except Exception as e:
                print(f"  [!] Error in final batch: {e}")

    total_time = time.time() - start_time
    avg_throughput = total_loaded / total_time if total_time > 0 else 0

    print(f"Batch loading completed: {total_loaded} new rows in {total_time:.2f}s | Throughput: {avg_throughput:.1f} rec/s\n")
    client.close()

    return {
        "engine": "python_batch",
        "loaded_raw": total_loaded,
        "seconds_elapsed": total_time,
        "throughput": avg_throughput,
        "batch_size": BATCH_SIZE
    }