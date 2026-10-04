import sys
import os
import time
from pymongo import MongoClient, ASCENDING, DESCENDING

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import MONGO_URI, DB_NAME, COLLECTION_VALIDATED

# Index Specifications
# 1. Single Field Index on customer_id: لتسريع استعلامات سجل طلبات العميل
INDEX_CUSTOMER = ("idx_customer_id", [("customer_id", ASCENDING)])

# 2. Single Field Index on order_date: لتسريع استعلامات النطاق الزمني والفرز التاريخي
INDEX_DATE = ("idx_order_date_desc", [("order_date", DESCENDING)])

# 3. Compound Index (مؤشر مركب) on city, status, order_date:
# يخدم استعلام تصفية الطلبات حسب المدينة والحالة مع ترتيب تاريخي
INDEX_COMPOUND_CITY_STATUS = (
    "idx_city_status_date",
    [("city", ASCENDING), ("status", ASCENDING), ("order_date", DESCENDING)]
)

# 4. Compound Index on payment_method and payment_status
INDEX_PAYMENT = (
    "idx_payment_method_status",
    [("payment_method", ASCENDING), ("payment_status", ASCENDING)]
)

INDEX_DEFINITIONS = [
    INDEX_CUSTOMER,
    INDEX_DATE,
    INDEX_COMPOUND_CITY_STATUS,
    INDEX_PAYMENT
]

def get_db():
    client = MongoClient(MONGO_URI)
    return client, client[DB_NAME]

def create_indexes():
    """
    إنشاء الفهارس المطلوبة (3 على الأقل مع مركب واحد على الأقل).
    """
    client, db = get_db()
    try:
        col = db[COLLECTION_VALIDATED]
        created = []
        for name, keys in INDEX_DEFINITIONS:
            idx_name = col.create_index(keys, name=name, background=True)
            created.append({"name": idx_name, "keys": keys})
        return {
            "status": "SUCCESS",
            "message": f"Successfully created {len(created)} indexes on {COLLECTION_VALIDATED}",
            "indexes": created
        }
    finally:
        client.close()

def drop_custom_indexes():
    """حذف الفهارس المخصصة لإجراء اختبارات Explain قبل وبعد الفهارس"""
    client, db = get_db()
    try:
        col = db[COLLECTION_VALIDATED]
        existing = col.index_information()
        dropped = []
        for name, _ in INDEX_DEFINITIONS:
            if name in existing:
                col.drop_index(name)
                dropped.append(name)
        return {"dropped": dropped}
    finally:
        client.close()

# -------------------------------------------------------------
# 5 Practical Queries (5 استعلامات عملية تناسب طبيعة بيانات الطلبات)
# -------------------------------------------------------------

def query_city_and_status(db, city="صنعاء", status="مؤكد", limit=20, explain=False):
    """
    Query 1: استعلام الطلبات حسب المدينة والحالة مع الترتيب التاريخي.
    مخدوم مباشرة بالمؤشر المركب: (city, status, order_date).
    """
    col = db[COLLECTION_VALIDATED]
    filter_criteria = {"city": city, "status": status}
    cursor = col.find(
        filter_criteria,
        {"_id": 0, "order_id": 1, "order_date": 1, "customer_name": 1, "total_amount": 1, "status": 1, "city": 1}
    ).sort("order_date", DESCENDING).limit(limit)

    if explain:
        return cursor.explain()
    return list(cursor)

def query_customer_history(db, customer_id="عميل-0", limit=20, explain=False):
    """
    Query 2: استعلام سجل طلبات عميل محدد.
    مخدوم بمؤشر: (customer_id).
    """
    col = db[COLLECTION_VALIDATED]
    cursor = col.find(
        {"customer_id": customer_id},
        {"_id": 0, "order_id": 1, "order_date": 1, "status": 1, "total_amount": 1, "payment_method": 1}
    ).sort("order_date", DESCENDING).limit(limit)

    if explain:
        return cursor.explain()
    return list(cursor)

def query_date_range(db, start_date="2025-01-01", end_date="2025-02-28", limit=20, explain=False):
    """
    Query 3: استعلام الطلبات ضمن نطاق زمني محدد.
    مخدوم بمؤشر: (order_date).
    """
    col = db[COLLECTION_VALIDATED]
    cursor = col.find(
        {"order_date": {"$gte": start_date, "$lte": end_date}},
        {"_id": 0, "order_id": 1, "order_date": 1, "city": 1, "total_amount": 1, "status": 1}
    ).sort("order_date", DESCENDING).limit(limit)

    if explain:
        return cursor.explain()
    return list(cursor)

def query_paid_by_payment_method(db, payment_method="محفظة إلكترونية", payment_status="تم الدفع", limit=20, explain=False):
    """
    Query 4: استعلام الطلبات المدفوعة حسب وسيلة الدفع.
    مخدوم بمؤشر: (payment_method, payment_status).
    """
    col = db[COLLECTION_VALIDATED]
    cursor = col.find(
        {"payment_method": payment_method, "payment_status": payment_status},
        {"_id": 0, "order_id": 1, "customer_id": 1, "payment_amount": 1, "currency": 1, "status": 1}
    ).limit(limit)

    if explain:
        return cursor.explain()
    return list(cursor)

def query_high_value_orders_by_delivery(db, delivery_type="سريع", limit=20, explain=False):
    """
    Query 5: استعلام الطلبات للتوصيل السريع ذات الأولوية.
    """
    col = db[COLLECTION_VALIDATED]
    cursor = col.find(
        {"delivery_type": delivery_type, "status": {"$in": ["مؤكد", "قيد الانتظار"]}},
        {"_id": 0, "order_id": 1, "customer_name": 1, "city": 1, "district": 1, "delivery_cost": 1, "total_amount": 1}
    ).limit(limit)

    if explain:
        return cursor.explain()
    return list(cursor)

QUERIES_REGISTRY = {
    "city_and_status": {
        "title": "الطلبات حسب المدينة والحالة",
        "description": "فلترة الطلبات حسب المدينة وحالة الطلب مع فرز زمني (يستفيد من Compound Index)",
        "func": query_city_and_status
    },
    "customer_history": {
        "title": "سجل طلبات العميل",
        "description": "جلب كافة الطلبات المرتبطة بمعرف عميل محدد (يستفيد من Index customer_id)",
        "func": query_customer_history
    },
    "date_range": {
        "title": "الطلبات ضمن نطاق زمني",
        "description": "استرجاع الطلبات المنفذة في فترة زمنية محددة (يستفيد من Index order_date)",
        "func": query_date_range
    },
    "paid_by_payment_method": {
        "title": "الطلبات المدفوعة حسب وسيلة الدفع",
        "description": "فلترة الطلبات المكتملة الدفع بنوع وسيلة الدفع (يستفيد من Index payment_method_status)",
        "func": query_paid_by_payment_method
    },
    "delivery_priority": {
        "title": "طلبات التوصيل السريع النشطة",
        "description": "استعلام الطلبات قيد المعالجة للتوصيل السريع",
        "func": query_high_value_orders_by_delivery
    }
}

def execute_query(name, params=None, explain=False):
    """تشغيل أي استعلام بالاسم مع دعم بارامترات مخصصة أو explain"""
    if name not in QUERIES_REGISTRY:
        raise ValueError(f"Unknown query name: {name}. Available: {list(QUERIES_REGISTRY.keys())}")
    
    client, db = get_db()
    try:
        q_info = QUERIES_REGISTRY[name]
        func = q_info["func"]
        kwargs = params or {}
        kwargs["explain"] = explain
        return func(db, **kwargs)
    finally:
        client.close()

# -------------------------------------------------------------
# Explain ExecutionStats Benchmarking (مقارنة الأداء قبل وبعد الفهارس)
# -------------------------------------------------------------

def extract_execution_stats(explain_plan):
    """استخراج أهم مؤشرات الأداء من خطة التنفيذ executionStats"""
    stats = explain_plan.get("executionStats", {})
    server_info = explain_plan.get("serverInfo", {})
    query_planner = explain_plan.get("queryPlanner", {})
    winning_plan = query_planner.get("winningPlan", {})

    def get_stages(plan):
        stages = []
        if isinstance(plan, dict):
            if "stage" in plan:
                stages.append(plan["stage"])
            if "inputStage" in plan:
                stages.extend(get_stages(plan["inputStage"]))
        return stages

    stages = get_stages(winning_plan)
    primary_stage = stages[0] if stages else "UNKNOWN"

    return {
        "execution_time_millis": stats.get("executionTimeMillis", 0),
        "total_docs_examined": stats.get("totalDocsExamined", 0),
        "total_keys_examined": stats.get("totalKeysExamined", 0),
        "n_returned": stats.get("nReturned", 0),
        "stage": primary_stage,
        "stages_chain": stages,
        "index_used": winning_plan.get("inputStage", {}).get("indexName") or winning_plan.get("indexName") or "None (COLLSCAN)"
    }

def run_explain_comparison():
    """
    تنفيذ executionStats لـ 3 استعلامات قبل وبعد إنشاء الفهارس
    مع توضيح سبب اختيار كل فهرس وأثره.
    """
    client, db = get_db()
    try:
        target_queries = ["city_and_status", "customer_history", "date_range"]
        results = {}

        # 1. اختبار قبل الفهارس (DROP INDEXES)
        drop_custom_indexes()
        # نترك مهلة قصيرة للتأكد من حذف الفهارس في الذاكرة المؤقتة
        time.sleep(0.5)

        before_stats = {}
        for q_name in target_queries:
            plan = execute_query(q_name, explain=True)
            before_stats[q_name] = extract_execution_stats(plan)

        # 2. إنشاء الفهارس (CREATE INDEXES)
        create_indexes()
        time.sleep(0.5)

        after_stats = {}
        for q_name in target_queries:
            plan = execute_query(q_name, explain=True)
            after_stats[q_name] = extract_execution_stats(plan)

        # 3. صياغة المقارنة والتحليل
        comparisons = []
        justifications = {
            "city_and_status": {
                "index_name": "idx_city_status_date (Compound)",
                "reason": "الاستعلام يقوم بالفلترة على حقلين معاً (المدينة والحالة) ثم الفرز التنازلي للتاريخ. الفهرس المركب يتيح الوصول الفوري دون مسح المجموعة (COLLSCAN) ويتجنب فرز الذاكرة (In-memory Sort).",
                "impact": f"تحول من {before_stats['city_and_status']['stage']} إلى {after_stats['city_and_status']['stage']}، وانخفضت الوثائق المفحوصة من {before_stats['city_and_status']['total_docs_examined']:,} إلى {after_stats['city_and_status']['total_docs_examined']:,}."
            },
            "customer_history": {
                "index_name": "idx_customer_id (Single Field)",
                "reason": "البحث عن طلبات عميل محدد في ملايين السجلات يتطلب قفزة مباشرة لمعرف العميل بدلاً من الفحص الكامل للوثائق.",
                "impact": f"تحول الفحص من {before_stats['customer_history']['stage']} إلى {after_stats['customer_history']['stage']}، وانخفض زمن التنفيذ بنسبة تسريع ملحوظة."
            },
            "date_range": {
                "index_name": "idx_order_date_desc (Single Field)",
                "reason": "استعلامات النطاق الزمني ($gte / $lte) تتطلب فحصاً مرتباً على التواريخ، مما يسمح للـ Query Engine بمسح نطاق محدد فقط في الـ B-Tree.",
                "impact": f"تخفيض عدد الوثائق المفحوصة من {before_stats['date_range']['total_docs_examined']:,} إلى {after_stats['date_range']['total_docs_examined']:,}، مما يضمن ثبات الأداء حتى مع كبر حجم البيانات."
            }
        }

        for q_name in target_queries:
            b = before_stats[q_name]
            a = after_stats[q_name]
            speedup = round(b["execution_time_millis"] / max(a["execution_time_millis"], 1), 2) if b["execution_time_millis"] > 0 else 1.0
            doc_reduction = round((1 - (a["total_docs_examined"] / max(b["total_docs_examined"], 1))) * 100, 1) if b["total_docs_examined"] > 0 else 0.0

            comparisons.append({
                "query_name": q_name,
                "title": QUERIES_REGISTRY[q_name]["title"],
                "index_used": justifications[q_name]["index_name"],
                "reason_for_index": justifications[q_name]["reason"],
                "performance_impact": justifications[q_name]["impact"],
                "before_index": b,
                "after_index": a,
                "metrics_comparison": {
                    "time_before_ms": b["execution_time_millis"],
                    "time_after_ms": a["execution_time_millis"],
                    "docs_examined_before": b["total_docs_examined"],
                    "docs_examined_after": a["total_docs_examined"],
                    "docs_reduction_pct": f"{doc_reduction}%",
                    "speedup_factor": f"{speedup}x"
                }
            })

        return {
            "status": "SUCCESS",
            "queries_benchmarked": len(comparisons),
            "comparisons": comparisons
        }
    finally:
        client.close()
