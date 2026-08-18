import sys
import os
import uuid
import time
import argparse

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.mongo_setup import init_mongo
from src.file_router import route_file
from src.batch_loader import run_batch_loader
from src.spark_loader import run_spark_loader
from src.elt_pipeline import process_elt_transformation
from src.metrics import save_metrics

def main():
    parser = argparse.ArgumentParser(description="Hybrid Big Data Pipeline CLI")
    parser.add_argument("--file", required=True, help="Path to input CSV file")
    args = parser.parse_args()

    init_mongo()
    engine, file_size_mb = route_file(args.file)
    run_id = str(uuid.uuid4())
    total_start = time.time()

    if engine == "python_batch":
        load_stats = run_batch_loader(args.file, run_id)
    else:
        load_stats = run_spark_loader(args.file, run_id)

    # تطبيق مرحلة التنظيف والتحويل المركزية الموحدة (ELT)
    elt_stats = process_elt_transformation(run_id, batch_chunk_size=5000)

    total_duration = time.time() - total_start

    final_report = {
        "run_id": run_id,
        "file_name": os.path.basename(args.file),
        "file_size_mb": round(file_size_mb, 2),
        "used_engine": engine,
        "read_rows": load_stats.get("loaded_raw", 0),
        "loaded_raw": load_stats.get("loaded_raw", 0),
        "count_valid": elt_stats["count_valid"],
        "count_corrected": elt_stats["count_corrected"],
        "count_quarantine": elt_stats["count_quarantine"],
        "count_inserted": elt_stats["count_inserted"],
        "count_updated": elt_stats["count_updated"],
        "count_unchanged": elt_stats["count_unchanged"],
        "seconds_elapsed": round(total_duration, 2),
        "throughput": round(load_stats.get("loaded_raw", 0) / total_duration, 2) if total_duration > 0 else 0,
        "engine_details": {
            "batch_size": load_stats.get("batch_size"),
            "partitions": load_stats.get("partitions")
        },
        "counts_case_error": elt_stats["counts_case_error"],
        "consistency_check": elt_stats["consistency_check"]
    }

    save_metrics(final_report)

if __name__ == "__main__":
    main()