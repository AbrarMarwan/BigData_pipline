import sys
import os
import uuid
import time
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query, Body, status
from fastapi.responses import JSONResponse
from pymongo import MongoClient

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    MONGO_URI,
    DB_NAME,
    COLLECTION_RAW,
    COLLECTION_VALIDATED,
    COLLECTION_QUARANTINE,
    MV_DAILY_SALES,
    MV_TOP_PRODUCTS,
    API_HOST,
    API_PORT
)
from src.mongo_setup import init_mongo
from src.file_router import route_file
from src.batch_loader import run_batch_loader
from src.spark_loader import run_spark_loader
from src.elt_pipeline import process_elt_transformation
from src.metrics import save_metrics
from src.queries_indexes import (
    create_indexes,
    execute_query,
    run_explain_comparison,
    QUERIES_REGISTRY
)
from src.aggregations import (
    execute_aggregation,
    AGGREGATIONS_REGISTRY
)
from src.materialized_views import (
    refresh_all_materialized_views,
    get_materialized_view_data
)
from src.scheduler import (
    init_scheduler,
    trigger_job_manually,
    get_job_history,
    get_scheduler_status,
    JOBS_REGISTRY
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # بدء تشغيل قاعدة البيانات والـ Scheduler تلقائياً مع السيرفر
    init_mongo()
    sched = init_scheduler()
    yield
    if sched.running:
        sched.shutdown(wait=False)

app = FastAPI(
    title="Big Data Hybrid Pipeline API - Phase 2",
    description="واجهة تشغيل موحدة للمشروع النهائي لاختبار خط البيانات، الاستعلامات، الفهارس، التجميعات، العروض المادية، والمهام المجدولة.",
    version="2.0.0",
    lifespan=lifespan
)

# -------------------------------------------------------------
# 1. Health Check Endpoint
# -------------------------------------------------------------
@app.get("/health", summary="فحص صحة النظام وقاعدة البيانات")
def get_health():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command('ping')
        db = client[DB_NAME]
        counts = {
            "raw": db[COLLECTION_RAW].estimated_document_count(),
            "validated": db[COLLECTION_VALIDATED].estimated_document_count(),
            "quarantine": db[COLLECTION_QUARANTINE].estimated_document_count(),
            "daily_sales_mv": db[MV_DAILY_SALES].estimated_document_count(),
            "top_products_mv": db[MV_TOP_PRODUCTS].estimated_document_count()
        }
        scheduler_status = get_scheduler_status()
        return {
            "status": "HEALTHY",
            "database": DB_NAME,
            "collections_estimated_count": counts,
            "scheduler": scheduler_status
        }
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"Database unreachable: {e}")
    finally:
        client.close()

# -------------------------------------------------------------
# 2. Ingestion Pipeline Endpoint (Midterm Pipeline Gateway)
# -------------------------------------------------------------
@app.post("/ingest", summary="تشغيل خط الإدخال والـ ELT على ملف البيانات")
def run_ingestion(
    file_path: str = Query("data/sample_batch.csv", description="مسار ملف الـ CSV المراد تحميله ومعالجته")
):
    """
    يستخدم نفس بوابة الإدخال والـ Pipeline المنفذة في المشروع النصفي
    (File Router -> Python Batch أو PySpark -> MongoDB Raw -> ELT Clean & Validate -> Idempotent Upsert)
    """
    if not os.path.exists(file_path):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"File not found: {file_path}")

    init_mongo()
    engine, file_size_mb = route_file(file_path)
    run_id = str(uuid.uuid4())
    total_start = time.time()

    if engine == "python_batch":
        load_stats = run_batch_loader(file_path, run_id)
    else:
        load_stats = run_spark_loader(file_path, run_id)

    elt_stats = process_elt_transformation(run_id)
    total_duration = round(time.time() - total_start, 2)

    final_report = {
        "run_id": run_id,
        "file_name": os.path.basename(file_path),
        "file_size_mb": round(file_size_mb, 2),
        "engine_used": engine,
        "rows_read": load_stats.get("loaded_raw", 0),
        "raw_loaded": load_stats.get("loaded_raw", 0),
        "valid_count": elt_stats["count_valid"],
        "corrected_count": elt_stats["count_corrected"],
        "quarantine_count": elt_stats["count_quarantine"],
        "inserted_count": elt_stats["count_inserted"],
        "updated_count": elt_stats["count_updated"],
        "unchanged_count": elt_stats["count_unchanged"],
        "elapsed_seconds": total_duration,
        "throughput": round(load_stats.get("loaded_raw", 0) / total_duration, 2) if total_duration > 0 else 0,
        "engine_details": {
            "batch_size": load_stats.get("batch_size"),
            "partitions": load_stats.get("partitions")
        },
        "error_case_counts": elt_stats["error_case_counts"],
        "consistency_check": elt_stats["consistency_check"]
    }
    save_metrics(final_report)
    return final_report

# -------------------------------------------------------------
# 3. Indexes Endpoint
# -------------------------------------------------------------
@app.post("/indexes", summary="إنشاء الفهارس (3 على الأقل مع مركب واحد)")
def post_create_indexes():
    """إنشاء فهارس customer_id, order_date, والمؤشر المركب city_status_date"""
    result = create_indexes()
    return result

# -------------------------------------------------------------
# 4. Queries Endpoints (5 Practical Queries + Explain)
# -------------------------------------------------------------
@app.get("/queries", summary="قائمة الاستعلامات العملية المتاحة")
def list_queries():
    queries_meta = {}
    for name, q in QUERIES_REGISTRY.items():
        queries_meta[name] = {
            "title": q["title"],
            "description": q["description"]
        }
    return {
        "count": len(queries_meta),
        "queries": queries_meta
    }

@app.get("/queries/explain/compare", summary="مقارنة executionStats قبل وبعد الفهارس لـ 3 استعلامات")
def explain_comparison():
    """
    تنفيذ executionStats لـ 3 استعلامات قبل وبعد إنشاء الفهارس
    مع توضيح أثر الفهرس ونسبة تسريع الاستعلام وتخفيض الوثائق المفحوصة
    """
    return run_explain_comparison()

@app.get("/queries/{name}", summary="تشغيل استعلام محدد بالاسم")
def run_query(
    name: str,
    city: Optional[str] = Query(None, description="فلترة حسب المدينة"),
    status: Optional[str] = Query(None, description="فلترة حسب حالة الطلب"),
    customer_id: Optional[str] = Query(None, description="معرف العميل"),
    start_date: Optional[str] = Query(None, description="تاريخ البداية (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="تاريخ النهاية (YYYY-MM-DD)"),
    delivery_type: Optional[str] = Query(None, description="نوع التوصيل"),
    payment_method: Optional[str] = Query(None, description="وسيلة الدفع"),
    limit: int = Query(20, ge=1, le=1000, description="الحد الأقصى للنتائج"),
    explain: bool = Query(False, description="إرجاع خطة التنفيذ executionStats بدلاً من البيانات")
):
    params = {}
    if city: params["city"] = city
    if status: params["status"] = status
    if customer_id: params["customer_id"] = customer_id
    if start_date: params["start_date"] = start_date
    if end_date: params["end_date"] = end_date
    if delivery_type: params["delivery_type"] = delivery_type
    if payment_method: params["payment_method"] = payment_method
    params["limit"] = limit

    try:
        data = execute_query(name, params=params, explain=explain)
        if explain:
            return data
        return {
            "query_name": name,
            "returned_count": len(data),
            "results": data
        }
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

# -------------------------------------------------------------
# 5. Aggregations Endpoints (5 Reports)
# -------------------------------------------------------------
@app.get("/aggregations", summary="قائمة تقارير الـ Aggregation المتاحة")
def list_aggregations():
    reports_meta = {}
    for name, r in AGGREGATIONS_REGISTRY.items():
        reports_meta[name] = {
            "title": r["title"],
            "description": r["description"]
        }
    return {
        "count": len(reports_meta),
        "reports": reports_meta
    }

@app.get("/aggregations/{name}", summary="تشغيل تقرير تجميعي محدد بالاسم")
def run_aggregation(
    name: str,
    limit: int = Query(20, ge=1, le=1000, description="الحد الأقصى للعناصر في التقرير")
):
    try:
        result = execute_aggregation(name, limit=limit)
        return {
            "report_name": name,
            "title": AGGREGATIONS_REGISTRY[name]["title"],
            "data": result
        }
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

# -------------------------------------------------------------
# 6. Materialized Views Endpoints
# -------------------------------------------------------------
@app.post("/refresh-mv", summary="تحديث العروض المادية (Materialized Views)")
def refresh_views(
    incremental: bool = Query(True, description="تحديث تزايدي باستخدام Watermark بدلاً من إعادة البناء الكاملة")
):
    """
    تحديث العرضين الماديين:
    1. daily_sales_summary
    2. top_products_summary
    """
    return refresh_all_materialized_views(incremental=incremental)

@app.get("/views/{name}", summary="استرجاع محتويات عرض مادي محدد مباشرة")
def get_view_data(
    name: str,
    limit: int = Query(50, ge=1, le=1000, description="الحد الأقصى للسجلات")
):
    try:
        data = get_materialized_view_data(name, limit=limit)
        return {
            "view_name": name,
            "count": len(data),
            "data": data
        }
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

# -------------------------------------------------------------
# 7. Scheduled Jobs Endpoints
# -------------------------------------------------------------
@app.get("/jobs", summary="قائمة المهام المجدولة وسجل التنفيذ التاريخي")
def list_jobs(
    limit: int = Query(20, ge=1, le=100, description="عدد سجلات التنفيذ الأخيرة")
):
    scheduler_info = get_scheduler_status()
    history = get_job_history(limit=limit)
    return {
        "scheduler_status": scheduler_info,
        "recent_executions_log": history
    }

@app.post("/jobs/{name}/run", summary="تشغيل مهمة مجدولة يدوياً واختبارها فوراً")
def run_job_manually(name: str):
    """
    تشغيل يدوي فوري للمهمة أثناء المناقشة والاختبار
    مع تسجيل نتيجة التنفيذ ووقت البداية والنهاية وحالة النجاح أو الفشل
    """
    try:
        result = trigger_job_manually(name)
        return {
            "job_name": name,
            "status": "SUCCESS",
            "execution_details": result
        }
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Job failed: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.api:app", host=API_HOST, port=API_PORT, reload=True)
