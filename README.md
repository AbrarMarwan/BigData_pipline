
# 🚀 Hybrid Big Data Pipeline for E-Commerce Operations

> **معمارية هجينة لمعالجة وتدفق البيانات الضخمة (ELT Pipeline) تجمع بين البث بالدفعات (Python Batch Streaming) والمعالجة الموزعة المتوازية (PySpark Engine) مع تطبيق صارم لقواعد جودة البيانات (Data Quality Gates) ومبدأ الـ Idempotency.**

---

## 📌 جدول المحتويات

* [نظرة عامة على المشروع](https://www.google.com/search?q=%23-%D9%86%D8%B8%D8%B1%D8%A9-%D8%B9%D8%A7%D9%85%D8%A9-%D8%B9%D9%84%D9%89-%D8%A7%D9%84%D9%85%D8%B4%D8%B1%D9%88%D8%B9)
* [المعمارية الهندسية (Architecture)](https://www.google.com/search?q=%23-%D8%A7%D9%84%D9%85%D8%B9%D9%85%D8%A7%D8%B1%D9%8A%D8%A9-%D8%A7%D9%84%D9%87%D9%86%D8%AF%D8%B3%D9%8A%D8%A9-architecture)
* [محرك التوجيه الذكي (Dynamic File Router)](https://www.google.com/search?q=%23-%D9%85%D8%AD%D8%B1%D9%83-%D8%A7%D9%84%D8%AA%D9%88%D8%AC%D9%8A%D9%87-%D8%A7%D9%84%D8%B0%D9%83%D9%8A-dynamic-file-router)
* [قواعد تنظيف وجودة البيانات (Data Quality Rules)](https://www.google.com/search?q=%23-%D9%82%D9%88%D8%A7%D8%B9%D8%AF-%D8%AA%D9%86%D8%B8%D9%8A%D9%81-%D9%88%D8%AC%D9%88%D8%AF%D8%A9-%D8%A7%D9%84%D8%A8%D9%8A%D8%A7%D9%86%D8%A7%D8%AA-data-quality-rules)
* [معمارية التخزين في MongoDB](https://www.google.com/search?q=%23-%D9%85%D8%B9%D9%85%D8%A7%D8%B1%D9%8A%D8%A9-%D8%A7%D9%84%D8%AA%D8%AE%D8%B2%D9%8A%D9%86-%D9%81%D9%8A-mongodb)
* [إثبات الـ Idempotency وتفادي التكرار](https://www.google.com/search?q=%23-%D8%A5%D8%AB%D8%A8%D8%A7%D8%AA-%D8%A7%D9%84%D9%80-idempotency-%D9%88%D8%AA%D9%81%D8%A7%D8%AF%D9%8A-%D8%A7%D9%84%D8%AA%D9%83%D8%B1%D8%A7%D8%B1)
* [نتائج التشغيل والتقييم (Benchmark Results)](https://www.google.com/search?q=%23-%D9%86%D8%AA%D8%A7%D8%A6%D8%AC-%D8%A7%D9%84%D8%AA%D8%B4%D8%BA%D9%8A%D9%84-%D9%88%D8%A7%D9%84%D8%AA%D9%82%D9%8A%D9%8A%D9%85-benchmark-results)
* [هيكل المشروع (Directory Structure)](https://www.google.com/search?q=%23-%D9%87%D9%8A%D9%83%D9%84-%D8%A7%D9%84%D9%85%D8%B4%D8%B1%D9%88%D8%B9-directory-structure)
* [دليل التثبيت والتشغيل](https://www.google.com/search?q=%23-%D8%AF%D9%84%D9%8A%D9%84-%D8%A7%D9%84%D8%AA%D8%AB%D8%A8%D9%8A%D8%AA-%D9%88%D8%A7%D9%84%D8%AA%D8%B4%D8%BA%D9%8A%D9%84)

---

## 💡 نظرة عامة على المشروع

تم تصميم هذا النظام لمعالجة تدفقات ضخمة من بيانات التجارة الإلكترونية المعقدة وغير النظيفة (Real-world dirty data). يطبق المشروع منهجية **ELT (Extract, Load, Transform)** الحديثة لضمان الاحتفاظ بالبيانات الخام الأصلية كاملة مع بناء طبقة معالجة فائقة السرعة تضمن الاتساق الرياضي ($100\%$ Consistency) وتوثيق التعديلات (Audit Trail).

---

## 🏗 المعمارية الهندسية (Architecture)

```text
               +----------------------------------+
               |     Input CSV Data Stream        |
               +-----------------+----------------+
                                 |
                     [ Dynamic File Router ]
                    /                       \
        Size <= 200 MB                     Size > 200 MB
              /                                   \
   +-----------------------+             +-----------------------+
   |  Python Batch Engine  |             |  PySpark Dist Engine  |
   | (Chunk-based Streaming)|             | (Partitioned Workers) |
   +-----------+-----------+             +-----------+-----------+
               \                                   /
                +-----------------+---------------+
                                  |
                                  v
                    +---------------------------+
                    | MongoDB: orders_raw Layer |  <--- Raw Data Lake
                    +-------------+-------------+
                                  |
                   [ In-Memory ELT Quality Gate ]
                     (Rules, Audit, Quarantine)
                                  |
                 +----------------+----------------+
                 |                                 |
                 v                                 v
   +---------------------------+     +---------------------------+
   | MongoDB: orders_validated |     | MongoDB: orders_quarantine|
   | (Unique Business Index)   |     | (Root Cause Error Log)    |
   +---------------------------+     +---------------------------+

```

---

## 🔀 محرك التوجيه الذكي (Dynamic File Router)

يقوم الموجه (`src/file_router.py`) بفحص حجم الملف الوارد وتحديد المحرك الأمثل تلقائياً:

* **الملفات الصغيرة والمتوسطة ($\le 200\text{ MB}$):** يوجهها إلى `Python Batch Loader` لمعالجة البيانات بالدفعات المتدفقة، لتفادي استهلاك موارد تشغيل الـ JVM في Spark.
* **الملفات الضخمة ($> 200\text{ MB}$):** يوجهها إلى محرك `PySpark` لتوزيع الحمل على نوى المعالج واستخدام المعالجة المتوازية (Parallel Ingestion).

---

## 🛡 قواعد تنظيف وجودة البيانات (Data Quality Rules)

| رمز القاعدة (Rule Code) | الوصف الهندسي والتنفيذي | الإجراء المتبع |
| --- | --- | --- |
| `ID_ORDER_MISSING` | فقدان أو تلف معرف الطلب (`order_id`) | عزل إلى Quarantine |
| `ID_CUSTOMER_MISSING` | فقدان معرف العميل (`customer_id`) | عزل إلى Quarantine |
| `DATE_IMPOSSIBLE_INVALID` | التاريخ خارج النطاق المنطقي أو غير قابل للتحليل | عزل إلى Quarantine |
| `JSON_ITEMS_CORRUPTED` | تلف تركيبي في هيكل الـ JSON للمنتجات | عزل إلى Quarantine |
| `ITEMS_EMPTY` | مصفوفة المنتجات فارغة `[]` أو غير معرفة | عزل إلى Quarantine |
| `DATE_STANDARDIZED` | توحيد صيغ التواريخ المختلفة وإزالة صيغة ISO-T | تصحيح + Audit Trail |
| `NUMBER_NORMALIZED` | تحويل الأرقام العربية، إزالة الفواصل، وتحويل الكلمات | تصحيح + Audit Trail |
| `CURRENCY_STANDARDIZED` | توحيد مسميات العملات المتنوعة إلى كود `YER` | تصحيح + Audit Trail |
| `EMAIL_REPEATED_SYMBOLS` | إزالة تكرار الرموز غير الصالحة مثل `@@` و `..` | تصحيح + Audit Trail |
| `PHONE_NORMALIZED` | توحيد صيغة أرقام الهواتف وإزالة المسافات ومفاتيح الدول | تصحيح + Audit Trail |
| `STRING_TRIMMED` | إزالة المسافات الزائدة وحروف الـ BOM الخفية (`\ufeff`) | تصحيح + Audit Trail |

---

## 🗄 معمارية التخزين في MongoDB

1. **`orders_raw`:**
* تخزين السجل الخام كما ورد من المصدر.
* إلحاق الميتاداتا الهندسية: `run_id`، `file_source`، `engine_used`، `at_ingested`.


2. **`orders_validated`:**
* تخزين السجلات المطابقة والمصححة.
* تحتوي على مصفوفة **`corrections`** (توثق الحقل، القيمة السابقة، القيمة المصححة، ورمز القاعدة).
* محكومة بفهرس فريد على مستوى قاعدة البيانات: `order_id_1 (Unique Index)`.


3. **`orders_quarantine`:**
* عزل السجلات غير القابلة للمعالجة.
* توثيق مصفوفة **`error_codes`** مع تفاصيل الخطأ `error_details` للتدقيق والتحليل المستقبلي.



---

## 🔁 إثبات الـ Idempotency وتفادي التكرار

* تم تطبيق نمط **Idempotent Upsert Pattern** باستخدام `UpdateOne` مع `upsert=True` بالاعتماد الحصري على مفتاح العمل الفريد `order_id`.
* عند إعادة تشغيل الخط أو وصول سجل مكرر، يقوم النظام بتحديث السجل الحالي بدلاً من إدخال صف جديد (`Updated Count`)، مما يضمن ثبات عدد الوثائق في `orders_validated` ومنع أي تكرار نهائياً.

---

## 📊 نتائج التشغيل والتقييم (Benchmark Results)

### 1. تجربة البث بالدفعات (Python Batch Streaming)

* **الملف:** `sample_orders.csv` ($2.09\text{ MB}$)
* **إجمالي السجلات الخام:** $5,000$
* **الاتساق الرياضي:** $4,776 \text{ (Corrected)} + 224 \text{ (Quarantine)} = 5,000 \text{ (Raw)} \implies \mathbf{100\%}$
* **الـ Upsert:** $4,737\text{ Inserted} \ \vert{} \ 39\text{ Updated (Deduplicated)}$
* **زمن التنفيذ:** $1.45\text{ s}$ ($\text{Throughput: } 3,441.7\text{ records/sec}$)

### 2. تجربة المعالجة الموزعة (PySpark Distributed Engine)

* **الملف:** `spark_1m.csv` ($418.55\text{ MB}$)
* **إجمالي السجلات الخام:** $1,000,000$
* **الاتساق الرياضي:** $992,910 \text{ (Corrected)} + 7,090 \text{ (Valid)} + 0 \text{ (Quarantine)} = 1,000,000 \implies \mathbf{100\%}$
* **الـ Upsert:** $986,061\text{ Inserted} \ \vert{} \ 13,939\text{ Updated (Deduplicated)}$
* **زمن التحميل والتوزيع المتوازي:** $55.28\text{ s}$ ($\text{Throughput: } 18,089.6\text{ records/sec}$)

---

## 📂 هيكل المشروع (Directory Structure)

```text
midterm-data-pipeline/
├── config/
│   └── settings.py              # إعدادات MongoDB والعتبات ومسارات النظام
├── data/
│   ├── sample_orders.csv        # عينة الاختبار السريع (Python Engine)
│   └── spark_1m.csv             # عينة المليون سجل (PySpark Engine)
├── reports/
│   ├── results.json             # ملف المقاييس والتقارير النهائي
│   └── screenshots/             # لقطات الشاشة الإثباتية من MongoDB Compass
├── src/
│   ├── batch_loader.py          # محرك القراءة بالدفعات المتدفقة
│   ├── elt_pipeline.py          # خط التحويل والتحقق من الجودة
│   ├── file_router.py           # موجه الملفات الذكي
│   ├── main.py                  # واجهة الأوامر ونقطة الانطلاق (CLI)
│   ├── metrics.py               # وحدة حساب وحفظ المقاييس والإحصائيات
│   ├── mongo_setup.py           # تهيئة المجموعات والفهارس الفريدة
│   ├── quality_rules.py         # القواعد الثمان لمعالجة وفحص الجودة
│   └── spark_loader.py          # محرك المعالجة المتوازية PySpark
├── requirements.txt             # المكتبات والاعتماديات
└── README.md                    # التوثيق الشامل للمشروع

```

---

## 💻 دليل التثبيت والتشغيل

### 1. تثبيت المتطلبات

```bash
pip install -r requirements.txt

```

### 2. تشغيل خط البيانات (CLI)

* **لتشغيل محرك البث بالدفعات (Python Batch):**
```bash
python src/main.py --file data/sample_orders.csv

```


* **لتشغيل محرك البيانات الضخمة (PySpark):**
```bash
python src/main.py --file data/spark_1m.csv

```



### 3. معاينة التقرير النهائي

تتم طباعة المقاييس كاملة في الطرفية، كما يتم تحديثها فورياً في الملف:

```bash
cat reports/results.json

```