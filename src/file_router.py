import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import SMALL_FILE_THRESHOLD_MB

def route_file(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    file_size_bytes = os.path.getsize(file_path)
    file_size_mb = file_size_bytes / (1024 * 1024)

    print(f"\n==========================================")
    print(f"📁 Target File: {os.path.basename(file_path)}")
    print(f"📦 Size: {file_size_mb:.2f} MB (Threshold: {SMALL_FILE_THRESHOLD_MB} MB)")

    if file_size_mb <= SMALL_FILE_THRESHOLD_MB:
        engine = "python_batch"
        reason = f"File size ({file_size_mb:.2f} MB) is <= {SMALL_FILE_THRESHOLD_MB} MB. Python Batch Streaming minimizes memory overhead."
    else:
        engine = "pyspark"
        reason = f"File size ({file_size_mb:.2f} MB) exceeds threshold. PySpark distributed engine selected for parallel processing."

    print(f"🚀 Selected Engine: {engine.upper()}")
    print(f"💡 Reason: {reason}")
    print(f"==========================================\n")

    return engine, file_size_mb