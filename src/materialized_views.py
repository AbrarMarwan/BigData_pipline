import sys
import os
import json
import time
from datetime import datetime
from bson import ObjectId
from pymongo import MongoClient, UpdateOne, ASCENDING

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    MONGO_URI,
    DB_NAME,
    COLLECTION_VALIDATED,
    MV_DAILY_SALES,
    MV_TOP_PRODUCTS,
    MV_METADATA
)

def get_db():
    client = MongoClient(MONGO_URI)
    return client, client[DB_NAME]

def init_materialized_views():
    """تهيئة الفهارس الخاصة بالعروض المادية لضمان كفاءة الـ Upsert وسرعة الاستعلام"""
    client, db = get_db()
    try:
        # 1. Unique index on daily_sales_summary (date)
        db[MV_DAILY_SALES].create_index([("date", ASCENDING)], unique=True, name="idx_mv_date_unique")
        
        # 2. Unique index on top_products_summary (sku)
        db[MV_TOP_PRODUCTS].create_index([("sku", ASCENDING)], unique=True, name="idx_mv_sku_unique")
        
        # 3. Unique index on mv_metadata (view_name)
        db[MV_METADATA].create_index([("view_name", ASCENDING)], unique=True, name="idx_mv_name_unique")
        return {"status": "SUCCESS", "message": "Materialized views indexes initialized."}
    finally:
        client.close()

def get_mv_watermark(db, view_name):
    """جلب آخر علامة مائية (Watermark) تم الوصول إليها للتحديث التزايدي"""
    meta = db[MV_METADATA].find_one({"view_name": view_name})
    if not meta:
        return None
    return meta.get("last_processed_id")

def update_mv_watermark(db, view_name, last_id, count_added):
    """تحديث العلامة المائية بعد اكتمال الدفعة التزايدية"""
    db[MV_METADATA].update_one(
        {"view_name": view_name},
        {
            "$set": {
                "view_name": view_name,
                "last_processed_id": last_id,
                "last_refresh_time": datetime.utcnow().isoformat()
            },
            "$inc": {"total_records_processed": count_added}
        },
        upsert=True
    )

def refresh_daily_sales_mv(incremental=True, batch_size=20000):
    """
    تحديث العرض المادي: daily_sales_summary
    يدعم التحديث التزايدي (Incremental) دون إعادة بناء كل شيء من الصفر.
    """
    client, db = get_db()
    start_time = time.time()
    try:
        init_materialized_views()
        valid_col = db[COLLECTION_VALIDATED]
        mv_col = db[MV_DAILY_SALES]

        query = {}
        last_id = None
        if incremental:
            last_id = get_mv_watermark(db, MV_DAILY_SALES)
            if last_id:
                try:
                    query["_id"] = {"$gt": ObjectId(last_id)}
                except Exception:
                    query["_id"] = {"$gt": last_id}

        # فرز حسب _id لضمان تتبع تسلسلي للعلامة المائية
        cursor = valid_col.find(
            query,
            {"_id": 1, "order_date": 1, "total_amount": 1}
        ).sort("_id", ASCENDING).limit(batch_size)

        daily_delta = {}
        processed_count = 0
        new_last_id = last_id

        for doc in cursor:
            doc_id = str(doc["_id"])
            new_last_id = doc_id
            processed_count += 1

            raw_date = doc.get("order_date", "")
            date_key = str(raw_date)[:10] if raw_date else "UNKNOWN_DATE"

            try:
                amt = float(doc.get("total_amount", 0.0) or 0.0)
            except Exception:
                amt = 0.0

            if date_key not in daily_delta:
                daily_delta[date_key] = {"total_revenue": 0.0, "order_count": 0}
            daily_delta[date_key]["total_revenue"] += amt
            daily_delta[date_key]["order_count"] += 1

        if not incremental:
            mv_col.delete_many({})

        # تطبيق التحديث التزايدي بواسطة Upsert و $inc
        bulk_ops = []
        for date_key, delta in daily_delta.items():
            bulk_ops.append(
                UpdateOne(
                    {"date": date_key},
                    {
                        "$inc": {
                            "total_revenue": delta["total_revenue"],
                            "order_count": delta["order_count"]
                        },
                        "$set": {
                            "updated_at": datetime.utcnow().isoformat()
                        }
                    },
                    upsert=True
                )
            )

        updated_count = 0
        if bulk_ops:
            res = mv_col.bulk_write(bulk_ops, ordered=False)
            updated_count = res.upserted_count + res.modified_count

            # إعادة حساب avg_order_value للتواريخ المعدلة لضمان الدقة الحسابية
            for date_key in daily_delta:
                doc = mv_col.find_one({"date": date_key})
                if doc and doc.get("order_count", 0) > 0:
                    avg_val = round(doc["total_revenue"] / doc["order_count"], 2)
                    mv_col.update_one(
                        {"date": date_key},
                        {"$set": {"avg_order_value": avg_val, "total_revenue": round(doc["total_revenue"], 2)}}
                    )

        if new_last_id:
            update_mv_watermark(db, MV_DAILY_SALES, new_last_id, processed_count)

        duration = round(time.time() - start_time, 2)
        return {
            "view_name": MV_DAILY_SALES,
            "mode": "incremental" if incremental else "full_rebuild",
            "new_records_processed": processed_count,
            "dates_updated": len(daily_delta),
            "last_watermark_id": new_last_id,
            "elapsed_seconds": duration,
            "status": "SUCCESS"
        }
    finally:
        client.close()

def refresh_top_products_mv(incremental=True, batch_size=20000):
    """
    تحديث العرض المادي: top_products_summary
    يدعم التحديث التزايدي (Incremental) دون إعادة بناء كل شيء من الصفر.
    """
    client, db = get_db()
    start_time = time.time()
    try:
        init_materialized_views()
        valid_col = db[COLLECTION_VALIDATED]
        mv_col = db[MV_TOP_PRODUCTS]

        query = {}
        last_id = None
        if incremental:
            last_id = get_mv_watermark(db, MV_TOP_PRODUCTS)
            if last_id:
                try:
                    query["_id"] = {"$gt": ObjectId(last_id)}
                except Exception:
                    query["_id"] = {"$gt": last_id}

        cursor = valid_col.find(
            query,
            {"_id": 1, "items_json": 1}
        ).sort("_id", ASCENDING).limit(batch_size)

        products_delta = {}
        processed_count = 0
        new_last_id = last_id

        for doc in cursor:
            doc_id = str(doc["_id"])
            new_last_id = doc_id
            processed_count += 1

            items_str = doc.get("items_json")
            if not items_str:
                continue
            try:
                items = json.loads(items_str)
                if not isinstance(items, list):
                    continue
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    sku = item.get("sku") or item.get("item_id") or "UNKNOWN_SKU"
                    name = item.get("name") or sku
                    qty = abs(int(item.get("qty", 1)))
                    unit_price = float(item.get("unit_price", item.get("price", 0.0)))
                    total = float(item.get("total", qty * unit_price))

                    if sku not in products_delta:
                        products_delta[sku] = {
                            "sku": sku,
                            "name": name,
                            "qty": 0,
                            "revenue": 0.0,
                            "appearances": 0
                        }
                    products_delta[sku]["qty"] += qty
                    products_delta[sku]["revenue"] += total
                    products_delta[sku]["appearances"] += 1
            except Exception:
                continue

        if not incremental:
            mv_col.delete_many({})

        bulk_ops = []
        for sku, p in products_delta.items():
            bulk_ops.append(
                UpdateOne(
                    {"sku": sku},
                    {
                        "$inc": {
                            "total_quantity_sold": p["qty"],
                            "total_revenue": p["revenue"],
                            "order_appearances": p["appearances"]
                        },
                        "$set": {
                            "product_name": p["name"],
                            "updated_at": datetime.utcnow().isoformat()
                        }
                    },
                    upsert=True
                )
            )

        if bulk_ops:
            mv_col.bulk_write(bulk_ops, ordered=False)
            # تقريب الإيرادات
            for sku in products_delta:
                doc = mv_col.find_one({"sku": sku})
                if doc:
                    mv_col.update_one(
                        {"sku": sku},
                        {"$set": {"total_revenue": round(doc["total_revenue"], 2)}}
                    )

        if new_last_id:
            update_mv_watermark(db, MV_TOP_PRODUCTS, new_last_id, processed_count)

        duration = round(time.time() - start_time, 2)
        return {
            "view_name": MV_TOP_PRODUCTS,
            "mode": "incremental" if incremental else "full_rebuild",
            "new_records_processed": processed_count,
            "products_updated": len(products_delta),
            "last_watermark_id": new_last_id,
            "elapsed_seconds": duration,
            "status": "SUCCESS"
        }
    finally:
        client.close()

def refresh_all_materialized_views(incremental=True):
    """تحديث جميع العروض المادية دفعة واحدة وتوثيق النتيجة"""
    res1 = refresh_daily_sales_mv(incremental=incremental)
    res2 = refresh_top_products_mv(incremental=incremental)
    return {
        "status": "SUCCESS",
        "timestamp": datetime.utcnow().isoformat(),
        "views": [res1, res2]
    }

def get_materialized_view_data(view_name, limit=50):
    """قراءة بيانات العرض المادي مباشرة بأعلى سرعة قراءة دون إعادة حساب"""
    client, db = get_db()
    try:
        if view_name == "daily_sales_summary":
            cursor = db[MV_DAILY_SALES].find({}, {"_id": 0}).sort("date", ASCENDING).limit(limit)
            return list(cursor)
        elif view_name == "top_products_summary":
            cursor = db[MV_TOP_PRODUCTS].find({}, {"_id": 0}).sort("total_revenue", -1).limit(limit)
            return list(cursor)
        else:
            raise ValueError(f"Unknown materialized view: {view_name}. Available: [daily_sales_summary, top_products_summary]")
    finally:
        client.close()
