import sys
import os
import time
import uuid
from datetime import datetime
from pymongo import MongoClient, DESCENDING
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import MONGO_URI, DB_NAME, COLLECTION_JOB_RUNS
from src.materialized_views import refresh_all_materialized_views
from src.aggregations import report_sales_by_city, report_orders_by_status

_scheduler = None

def get_db():
    client = MongoClient(MONGO_URI)
    return client, client[DB_NAME]

def log_job_execution(job_name, run_id, start_time, end_time, status, details=None, error=None):
    """تسجيل نتيجة تنفيذ المهمة المجدولة في مجموعة job_runs في MongoDB"""
    client, db = get_db()
    try:
        col = db[COLLECTION_JOB_RUNS]
        duration = round(end_time - start_time, 3)
        doc = {
            "run_id": run_id,
            "job_name": job_name,
            "start_time": datetime.utcfromtimestamp(start_time).isoformat(),
            "end_time": datetime.utcfromtimestamp(end_time).isoformat(),
            "duration_seconds": duration,
            "status": status,  # SUCCESS or FAILURE
            "details": details or {},
            "error": str(error) if error else None
        }
        col.insert_one(doc)
        return doc
    finally:
        client.close()

# -------------------------------------------------------------
# Scheduled Job 1: تحديث العروض المادية دورياً
# -------------------------------------------------------------
def task_refresh_materialized_views(run_id=None):
    """مهمة مجدولة لتحديث العروض المادية تزايدياً"""
    if not run_id:
        run_id = str(uuid.uuid4())
    start_time = time.time()
    try:
        result = refresh_all_materialized_views(incremental=True)
        end_time = time.time()
        log_job_execution(
            job_name="refresh_materialized_views",
            run_id=run_id,
            start_time=start_time,
            end_time=end_time,
            status="SUCCESS",
            details=result
        )
        return {"status": "SUCCESS", "run_id": run_id, "result": result}
    except Exception as e:
        end_time = time.time()
        log_job_execution(
            job_name="refresh_materialized_views",
            run_id=run_id,
            start_time=start_time,
            end_time=end_time,
            status="FAILURE",
            error=str(e)
        )
        raise e

# -------------------------------------------------------------
# Scheduled Job 2: إعداد التقرير الدوري للمبيعات وحالات الطلب
# -------------------------------------------------------------
def task_periodic_sales_audit(run_id=None):
    """مهمة مجدولة لإنشاء تقرير تدقيق دوري لملخص المبيعات وحالات الطلبات"""
    if not run_id:
        run_id = str(uuid.uuid4())
    start_time = time.time()
    try:
        top_cities = report_sales_by_city(limit=5)
        status_dist = report_orders_by_status()
        end_time = time.time()
        summary = {
            "top_city": top_cities[0] if top_cities else None,
            "total_orders_audited": status_dist.get("total_orders", 0),
            "status_summary": status_dist.get("distribution", [])[:3]
        }
        log_job_execution(
            job_name="periodic_sales_audit",
            run_id=run_id,
            start_time=start_time,
            end_time=end_time,
            status="SUCCESS",
            details=summary
        )
        return {"status": "SUCCESS", "run_id": run_id, "summary": summary}
    except Exception as e:
        end_time = time.time()
        log_job_execution(
            job_name="periodic_sales_audit",
            run_id=run_id,
            start_time=start_time,
            end_time=end_time,
            status="FAILURE",
            error=str(e)
        )
        raise e

JOBS_REGISTRY = {
    "refresh_materialized_views": {
        "title": "تحديث العروض المادية (Materialized Views)",
        "description": "تحديث تزايدي لملخص المبيعات اليومية وأفضل المنتجات عبر Watermark",
        "func": task_refresh_materialized_views,
        "interval_minutes": 15
    },
    "periodic_sales_audit": {
        "title": "تدقيق المبيعات الدوري (Periodic Sales Audit)",
        "description": "توليد كشوفات دورية لأعلى المدن وحالات الطلب التشغيلية",
        "func": task_periodic_sales_audit,
        "interval_minutes": 60
    }
}

def trigger_job_manually(job_name):
    """تشغيل أي مهمة يدوياً أثناء المناقشة والاختبار مع تسجيل النتيجة فوراً"""
    if job_name not in JOBS_REGISTRY:
        raise ValueError(f"Job '{job_name}' not found. Available jobs: {list(JOBS_REGISTRY.keys())}")
    func = JOBS_REGISTRY[job_name]["func"]
    run_id = f"manual-{uuid.uuid4()}"
    return func(run_id=run_id)

def get_job_history(limit=20, job_name=None):
    """استرجاع سجل تنفيذ المهام من MongoDB"""
    client, db = get_db()
    try:
        col = db[COLLECTION_JOB_RUNS]
        query = {"job_name": job_name} if job_name else {}
        cursor = col.find(query, {"_id": 0}).sort("start_time", DESCENDING).limit(limit)
        return list(cursor)
    finally:
        client.close()

def init_scheduler():
    """تهيئة وبدء تشغيل الـ Scheduler في الخلفية"""
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        return _scheduler

    _scheduler = BackgroundScheduler(daemon=True)

    # 1. Job 1: كل 15 دقيقة
    _scheduler.add_job(
        task_refresh_materialized_views,
        trigger=IntervalTrigger(minutes=JOBS_REGISTRY["refresh_materialized_views"]["interval_minutes"]),
        id="job_refresh_mv",
        name="Refresh Materialized Views",
        replace_existing=True
    )

    # 2. Job 2: كل 60 دقيقة
    _scheduler.add_job(
        task_periodic_sales_audit,
        trigger=IntervalTrigger(minutes=JOBS_REGISTRY["periodic_sales_audit"]["interval_minutes"]),
        id="job_sales_audit",
        name="Periodic Sales Audit",
        replace_existing=True
    )

    _scheduler.start()
    return _scheduler

def get_scheduler_status():
    """معلومات حالة الـ Scheduler والمهام المجدولة وقتاً ومواعيد التنفيذ القادمة"""
    global _scheduler
    is_running = _scheduler is not None and _scheduler.running
    jobs_info = []

    for name, meta in JOBS_REGISTRY.items():
        job_obj = _scheduler.get_job(f"job_{name.split('_')[1]}") if is_running else None
        next_run = job_obj.next_run_time.isoformat() if job_obj and job_obj.next_run_time else None
        jobs_info.append({
            "job_name": name,
            "title": meta["title"],
            "description": meta["description"],
            "interval_minutes": meta["interval_minutes"],
            "next_run_time": next_run
        })

    return {
        "scheduler_running": is_running,
        "jobs": jobs_info
    }
