import sys
import os
import json
from pymongo import MongoClient

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import MONGO_URI, DB_NAME, COLLECTION_VALIDATED

def get_db():
    client = MongoClient(MONGO_URI)
    return client, client[DB_NAME]

# -------------------------------------------------------------
# 5 Aggregation Reports (خمسة تقارير تجميعية متوافقة مع متطلبات المشروع)
# -------------------------------------------------------------

def report_sales_by_city(limit=20):
    """
    التقرير 1: المبيعات حسب المدينة (sales_by_city)
    يحسب: إجمالي الإيرادات، عدد الطلبات، متوسط قيمة الطلب لكل مدينة.
    """
    client, db = get_db()
    try:
        col = db[COLLECTION_VALIDATED]
        pipeline = [
            {
                "$addFields": {
                    "numeric_total": {
                        "$convert": {
                            "input": "$total_amount",
                            "to": "double",
                            "onError": 0.0,
                            "onNull": 0.0
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$city",
                    "total_revenue": {"$sum": "$numeric_total"},
                    "order_count": {"$sum": 1},
                    "avg_order_value": {"$avg": "$numeric_total"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "city": "$_id",
                    "total_revenue": {"$round": ["$total_revenue", 2]},
                    "order_count": 1,
                    "avg_order_value": {"$round": ["$avg_order_value", 2]}
                }
            },
            {"$sort": {"total_revenue": -1}},
            {"$limit": limit}
        ]
        return list(col.aggregate(pipeline))
    finally:
        client.close()

def report_top_products(limit=15):
    """
    التقرير 2: أفضل المنتجات مبيعاً وإيراداً (top_products)
    يقوم بفك مصفوفة عناصر الطلب (items_json)، وتجميع الكميات المباعة والإيرادات لكل منتج.
    """
    client, db = get_db()
    try:
        col = db[COLLECTION_VALIDATED]
        # نقوم بمعالجة آمنة وسريعة باستخدام Aggregation أو فحص العناصر
        # نظراً لأن items_json مخزن كنص JSON في السجلات المعالجة
        pipeline = [
            {"$match": {"items_json": {"$exists": True, "$ne": ""}}},
            {"$limit": 50000},  # أخذ عينة كبيرة ممثلة للسرعة في التقارير التفاعلية
            {"$project": {"items_json": 1}}
        ]
        cursor = col.aggregate(pipeline)
        
        products_map = {}
        for doc in cursor:
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
                    
                    if sku not in products_map:
                        products_map[sku] = {
                            "sku": sku,
                            "name": name,
                            "total_quantity_sold": 0,
                            "total_revenue": 0.0,
                            "order_appearances": 0
                        }
                    products_map[sku]["total_quantity_sold"] += qty
                    products_map[sku]["total_revenue"] += total
                    products_map[sku]["order_appearances"] += 1
            except Exception:
                continue

        result = list(products_map.values())
        result.sort(key=lambda x: x["total_revenue"], reverse=True)
        for r in result:
            r["total_revenue"] = round(r["total_revenue"], 2)
        return result[:limit]
    finally:
        client.close()

def report_top_customers(limit=20):
    """
    التقرير 3: أفضل العملاء إنفاقاً وتكراراً للطلبات (top_customers)
    """
    client, db = get_db()
    try:
        col = db[COLLECTION_VALIDATED]
        pipeline = [
            {
                "$addFields": {
                    "numeric_total": {
                        "$convert": {
                            "input": "$total_amount",
                            "to": "double",
                            "onError": 0.0,
                            "onNull": 0.0
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$customer_id",
                    "customer_name": {"$first": "$customer_name"},
                    "primary_city": {"$first": "$city"},
                    "total_spent": {"$sum": "$numeric_total"},
                    "order_count": {"$sum": 1},
                    "avg_spent_per_order": {"$avg": "$numeric_total"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "customer_id": "$_id",
                    "customer_name": 1,
                    "primary_city": 1,
                    "total_spent": {"$round": ["$total_spent", 2]},
                    "order_count": 1,
                    "avg_spent_per_order": {"$round": ["$avg_spent_per_order", 2]}
                }
            },
            {"$sort": {"total_spent": -1}},
            {"$limit": limit}
        ]
        return list(col.aggregate(pipeline))
    finally:
        client.close()

def report_sales_by_period(period_format="%Y-%m", limit=30):
    """
    التقرير 4: المبيعات حسب الفترة الزمنية (sales_by_period)
    تجميع إجمالي الإيرادات وعدد الطلبات شهرياً أو يومياً.
    """
    client, db = get_db()
    try:
        col = db[COLLECTION_VALIDATED]
        pipeline = [
            {
                "$addFields": {
                    "numeric_total": {
                        "$convert": {
                            "input": "$total_amount",
                            "to": "double",
                            "onError": 0.0,
                            "onNull": 0.0
                        }
                    },
                    "period": {
                        "$substrCP": ["$order_date", 0, 7]  # YYYY-MM
                    }
                }
            },
            {
                "$group": {
                    "_id": "$period",
                    "total_sales": {"$sum": "$numeric_total"},
                    "order_count": {"$sum": 1},
                    "avg_order_value": {"$avg": "$numeric_total"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "period": "$_id",
                    "total_sales": {"$round": ["$total_sales", 2]},
                    "order_count": 1,
                    "avg_order_value": {"$round": ["$avg_order_value", 2]}
                }
            },
            {"$sort": {"period": 1}},
            {"$limit": limit}
        ]
        return list(col.aggregate(pipeline))
    finally:
        client.close()

def report_orders_by_status(limit=None, **kwargs):
    """
    التقرير 5: توزيع الطلبات حسب الحالة التشغيلية (orders_by_status)
    إحصائيات حالات الطلب: مؤكد، قيد الانتظار، مرتجع، إلخ مع النسب المئوية.
    """
    client, db = get_db()
    try:
        col = db[COLLECTION_VALIDATED]
        pipeline = [
            {
                "$addFields": {
                    "numeric_total": {
                        "$convert": {
                            "input": "$total_amount",
                            "to": "double",
                            "onError": 0.0,
                            "onNull": 0.0
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$status",
                    "count": {"$sum": 1},
                    "total_amount": {"$sum": "$numeric_total"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "status": "$_id",
                    "count": 1,
                    "total_amount": {"$round": ["$total_amount", 2]}
                }
            },
            {"$sort": {"count": -1}}
        ]
        stats = list(col.aggregate(pipeline))
        total_orders = sum(s["count"] for s in stats)
        for s in stats:
            s["percentage"] = round((s["count"] / total_orders * 100), 2) if total_orders > 0 else 0.0
        return {
            "total_orders": total_orders,
            "distribution": stats
        }
    finally:
        client.close()

AGGREGATIONS_REGISTRY = {
    "sales_by_city": {
        "title": "المبيعات حسب المدينة",
        "description": "تقرير إجمالي المبيعات وعدد الطلبات ومتوسط قيمة الطلب لكل مدينة",
        "func": report_sales_by_city
    },
    "top_products": {
        "title": "أفضل المنتجات",
        "description": "تقرير المنتجات الأكثر مبيعاً وتحقيقاً للإيرادات من واقع عناصر الطلبات",
        "func": report_top_products
    },
    "top_customers": {
        "title": "أفضل العملاء",
        "description": "تقرير كبار العملاء إنفاقاً وتكراراً للشراء مع تفاصيل المدينة",
        "func": report_top_customers
    },
    "sales_by_period": {
        "title": "المبيعات حسب الفترة",
        "description": "تقرير الاتجاه الزمني لحجم المبيعات والإيرادات شهرياً",
        "func": report_sales_by_period
    },
    "orders_by_status": {
        "title": "توزيع الطلبات حسب الحالة",
        "description": "تقرير تحليل نسب وأعداد الطلبات لكل حالة تشغيلية",
        "func": report_orders_by_status
    }
}

def execute_aggregation(name, **kwargs):
    """تشغيل أي تقرير تجميعي بالاسم"""
    if name not in AGGREGATIONS_REGISTRY:
        raise ValueError(f"Unknown aggregation name: {name}. Available: {list(AGGREGATIONS_REGISTRY.keys())}")
    func = AGGREGATIONS_REGISTRY[name]["func"]
    return func(**kwargs)
