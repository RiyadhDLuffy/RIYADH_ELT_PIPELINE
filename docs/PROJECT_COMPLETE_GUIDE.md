# دليل المشروع الشامل ومرجع المناقشة الشفهية
## Al-Razi University – Big Data Practical Midterm Project
### Hybrid ELT Data Pipeline (Python Batch | Apache Spark | MongoDB)
**المشرف الأكاديمي: م. عمر أبوسند**

---

## 📑 فهرس المحتويات
1. [نظرة عامة وفلسفة المعمارية (ELT Philosophy & Architecture)](#1-نظرة-عامة-وفلسفة-المعمارية)
2. [دليل الملفات والدوال سطراً بسطر (File-by-File & Function-by-Function Deep Dive)](#2-دليل-الملفات-والدوال-سطرا-بسطر)
3. [قواعد التنظيف التسع والعزل وأثر التصحيح (9 Quality Rules & Quarantine)](#3-قواعد-التنظيف-التسع-والعزل-وأثر-التصحيح)
4. [المسار المتقدم B: التحميل التزايدي (Track B: Incremental Delta Loading)](#4-المسار-المتقدم-b-التحميل-التزايدي)
5. [سيناريوهات "ماذا لو؟" (What-If Analysis & Edge Cases)](#5-سيناريوهات-ماذا-لو)
6. [بنك الأسئلة المتوقعة في المناقشة الشفهية مع الإجابات النموذجية (Doctor Viva Q&A)](#6-بنك-الأسئلة-المتوقعة-في-المناقشة-الشفهية)
7. [دليل أوامر التشغيل ورفع المشروع إلى GitHub والتعامل مع ملفات جديدة](#7-دليل-أوامر-التشغيل-ورفع-المشروع)

---

## 1. نظرة عامة وفلسفة المعمارية

### لماذا ELT وليس ETL؟
* **ETL التقليدي (Extract -> Transform -> Load)**: يقوم بتنظيف البيانات وفلترتها قبل تخزينها، مما يؤدي إلى **فقدان البيانات غير الصالحة** واستحالة إعادة تحليل السجلات الخام في حال تغيرت قواعد العمل لاحقاً.
* **نمطنا ELT (Extract -> Load -> Transform)**:
  1. **التحميل الأولي 100% الخام**: كل سجل يصل من ملف CSV يُكتب أولاً فوراً كما هو في مجموعة `orders_raw` دون أي حذف أو تعديل.
  2. **التحويل والتصنيف اللاحق**: يتم تطبيق قواعد الجودة والتنظيف داخل الذاكرة مع حفظ مسار التدقيق (`Audit Trail`)، وتصنيف السجلات إلى `VALID` أو `CORRECTED` لتذهب إلى `orders_validated`، أو `QUARANTINE` لتذهب إلى `orders_quarantine`.
  3. **عدم فقدان أي معلومة**: تحقيق معادلة الاتساق الإلزامية:
     $$\text{rows\_read} = \text{valid\_count} + \text{corrected\_count} + \text{quarantine\_count}$$

### الموجّه التلقائي للمحرك (Hybrid Engine Router):
* **الملفات الصغيرة ($\le 200\text{ MB}$)**: يتم توجيهها تلقائياً إلى محرك **`Python Batch Loader`**. يتميز بانعدام وقت إقلاع الـ JVM واستخدام تقنية `Streaming` عبر `csv.DictReader` مع دفعات `bulk_write`، مما يمنح استهلاك ذاكرة ثابت جداً وسرعة فائقة.
* **الملفات الكبيرة ($> 200\text{ MB}$)**: يتم توجيهها إلى محرك **`PySpark Loader`** للاستفادة من المعالجة المتوازية وتوزيع البيانات على أنوية المعالج المتعددة عبر الـ RDD Partitions مع كتابة مباشرة لكل منفّذ إلى MongoDB لتجنب أي اختناق في الـ Driver.

### 📊 إحصائيات الملف الكبير الرسمية (30 مليون سجل — `orders_huge_mixed_quality.csv`):

| التصنيف (Classification) | العدد الدقيق (Exact Count) | النسبة المئوية (Percentage) | المعنى (Meaning) |
|---|---|---|---|
| **إجمالي السجلات الخام (Total Raw)** | **30,209,432** | **100.00%** | جميع السجلات كما وصلت من المصدر |
| **✅ السجلات السليمة (Valid)** | **22,886,416** | **75.76%** | سليمة 100% ولا تحتاج أي تعديل |
| **🔄 السجلات المصححة (Corrected)** | **4,345,804** | **14.39%** | تم تصحيحها آلياً وتوثيق أثر التعديل (Audit Trail) |
| **⚠️ السجلات المعزولة (Quarantined)** | **2,977,212** | **9.86%** | معزولة لوجود أخطاء جسيمة تمنع المعالجة |
| **الاتساق الرياضي (Consistency Check)** | **✅ PASSED** | — | $\text{Raw} = \text{Valid} + \text{Corrected} + \text{Quarantine}$ |

$$\mathbf{30,209,432} = \mathbf{22,886,416} + \mathbf{4,345,804} + \mathbf{2,977,212}$$

---

## 2. دليل الملفات والدوال سطراً بسطر

### 1️⃣ `main.py` (نقطة الدخول الرئيسية)
* **الوظيفة**: المدخل الرئيسي لتشغيل المشروع عبر سطر الأوامر.
* **الأسطر الهامة**:
  * ضبط `sys.stdout.reconfigure(encoding="utf-8")` لمنع انهيار الترميز عند طباعة النصوص العربية في بيئة Windows.
  * إضافة المجلد الجذري إلى `sys.path`.
  * استدعاء دالة `main()` من `src.elt_pipeline`.

---

### 2️⃣ `config/settings.py` (الإعدادات المركزية)
* **الوظيفة**: يجمع جميع الثوابت والمتغيرات البيئية ومسارات المجلدات.
* **المتغيرات الأساسية**:
  * `MONGO_URI`: رابط الاتصال بـ MongoDB (`mongodb://localhost:27017`).
  * `DB_NAME`: اسم قاعدة البيانات (`ecommerce_store`).
  * المجموعات الأربع: `orders_raw`, `orders_validated`, `orders_quarantine`, `pipeline_audit_logs`.
  * `SMALL_FILE_THRESHOLD_MB = 200`: الحد الفاصل لتوجيه الملفات.
  * `BATCH_SIZE = 2000`: حجم دفعة الكتابة الجماعية.
  * `ARABIC_DIGITS_TABLE`: جدول ترجمة الأرقام الهندية/العربية (`٠-٩`) إلى أرقام لاتينية (`0-9`).
  * `ARABIC_WORDS_TO_NUMBERS`: قاموس تحويل الكلمات العربية (مثل "ألفان" $\to 2000$, "خمسة آلاف" $\to 5000$).
  * `STATUS_SYNONYMS`: قاموس توحيد حالات الطلب والدفع وطرق التوصيل باللغة العربية إلى المعيار الإنجليزي الموحد (`CONFIRMED`, `PAID`, `CASH`, `DELIVERED`, إلخ).

---

### 3️⃣ `src/file_router.py` (موجّه الملفات الذكي)
* **الكلاس `RouteDecision`**:
  * يُرجع كائن بيانات يحتوي على: `file_path`, `size_bytes`, `size_mb`, `engine`, `reason`.
* **الدالة `route_file(file_path_str, threshold_mb, engine_override)`**:
  * تفحص وجود الملف وحجمه الفعلي على القرص عبر `path.stat().st_size`.
  * إذا تم تمرير خيار يدوي (`--engine python_batch` أو `--engine pyspark`) تعتمد الخيار اليدوي فوراً مع توضيح السبب.
  * إذا لم يتم التمرير اليدوي، تقارن `size_mb` بالحد `threshold_mb` (200MB):
    * أصغر أو يساوي: تختار `python_batch` وتبرر ذلك بخفة المحرك وانعدام الـ Overhead.
    * أكبر: تختار `pyspark` وتبرر ذلك بالاستفادة من الأنوية المتعددة والتوزيع.

---

### 4️⃣ `src/mongo_setup.py` (إعداد قاعدة البيانات والفهارس)
* **الدالة `setup_collections_and_indexes(db)`**:
  * تقوم بإنشاء الفهارس الضرورية للسرعة ومنع التكرار:
  1. `orders_raw`: فهرس على `run_id` و `ingested_at` (بدون Unique Index للسماح بحفظ تاريخ المحاولات كاملاً).
  2. `orders_validated`: **Unique Index على `order_id`** (المفتاح التجاري الثابت لضمان الـ Idempotency).
  3. `orders_quarantine`: **Unique Index على `issue_key`** لمنع تكرار عزل نفس السجل.
  4. `pipeline_audit_logs`: Unique Index على `run_id`.
* **الدالة `reset_database(db)`**:
  * تُسقط المجموعات وتعيد بناء الفهارس من الصفر عند طلب `--reset-db` لإجراء اختبارات أداء نظيفة.

---

### 5️⃣ `src/quality_rules.py` (محرك قواعد الجودة والتصنيف)
* **الدوال المساعدة**:
  * `as_text(val)`: تنظيف القيمة من `None`, `NaN`, `null`, والمسافات الزائدة.
  * `generate_issue_key(order_id, raw_record)`: توليد مفتاح فريد لعزل السجل، إما `order:{order_id}` أو تجزئة SHA-256 لمحتوى السجل إذا كان `order_id` مفقوداً.
* **دوال التنظيف الفردية (مع تسجيل أثر التصحيح Audit Trail)**:
  * `clean_arabic_and_text_number()`: القواعد 1 و 3 و 4 (الأرقام العربية، الفواصل، الكلمات).
  * `clean_currency_and_amount()`: القاعدة 2 (استخراج العملة وتوحيدها إلى YER وإزالة النصوص).
  * `clean_phone_number()`: القاعدة 5 (توحيد الهاتف إلى `967XXXXXXXXX`).
  * `clean_email()`: القاعدة 6 (إصلاح الرموز المكررة `@@` $\to$ `@`, `..` $\to$ `.`).
  * `clean_date()`: القاعدة 7 (تحويل التواريخ إلى صيغة ISO `YYYY-MM-DD` والتحقق من النطاق 1990-2030).
  * `clean_status_and_synonyms()`: القاعدة 8 (ترجمة المرادفات العربية وإزالة المسافات).
  * `parse_and_validate_items()`: فحص صحة حقل `items_json` وتفكيك عناصره والتحقق من الكميات والأسعار.
* **الدالة الرئيسية `transform_and_classify_row(raw, run_id)`**:
  * تطبق جميع القواعد وتجمع قائمة `corrections` وقائمة `errors`.
  * تطبق **القاعدة 9** (`TOTAL_RECONCILED`): إعادة حساب إجمالي الطلب بمطابقة مجموع العناصر + تكلفة التوصيل.
  * إذا وجدت أخطاء جوهرية (`MISSING_ORDER_ID`, `CORRUPTED_ITEMS_JSON`, إلخ): تُرجع `("QUARANTINE", None, quarantine_doc)`.
  * إذا لم توجد أخطاء ولكن وُجدت تصحيحات: تُرجع `("CORRECTED", validated_doc, None)`.
  * إذا كان السجل سليماً 100%: تُرجع `("VALID", validated_doc, None)`.

---

### 6️⃣ `src/batch_loader.py` (محرك Python الدفعي)
* **الدالة `process_python_batch(file_path, db, run_id, batch_size=2000)`**:
  * تفتح الملف بأسلوب `Streaming` باستخدام `csv.DictReader` دون تحميل الملف في الذاكرة.
  * لكل صف:
    1. تُنشئ وثيقة `raw_doc` وتضيفها لمصفوفة `raw_batch`.
    2. تُصنف السجل وتجهّز عملية `UpdateOne(..., upsert=True)`.
  * عندما يصل حجم المصفوفة إلى `batch_size`:
    * تُنفذ `raw_col.insert_many(raw_batch, ordered=False)`.
    * تُنفذ `val_col.bulk_write(val_ops, ordered=False)` و `quar_col.bulk_write(quar_ops, ordered=False)`.
    * تحسب وتطبع معدل الإدخال اللحظي `rows/sec`.
  * تُرجع قاموساً شاملاً بالمقاييس والعدادات (`rows_read`, `valid_count`, `corrected_count`, `quarantine_count`, `inserted_count`, `updated_count`, `unchanged_count`, `is_consistent`).

---

### 7️⃣ `src/spark_loader.py` (محرك Apache Spark الموزع)
* **الدالة `process_pyspark_batch(file_path, db, run_id, target_partitions=16)`**:
  * تبني `SparkSession` بالوضع المحلي مع استخدام جميع الأنوية `master("local[*]")`.
  * تستخدم **Fixed Permissive Schema** لقراءة جميع الحقول كـ `StringType` لمنع تشويه البيانات قبل مرحلة التحويل.
  * دالة **`partition_worker(partition_rows)`**:
    * تعمل داخل كل Spark Executor بشكل مستقل.
    * تفتح اتصالاً مباشراً بـ MongoDB وتكتب البيانات بدفعات `bulk_write` مباشرة دون إرسال البيانات الضخمة إلى الـ Driver.
    * تُرجع للـ Driver فقط ملخصاً صغيراً جداً بالأرقام والعدادات (`yield summary_dict`).
  * تضمن هذه التقنية **عدم حدوث Out-Of-Memory (OOM)** على الـ Driver نهائياً مهما بلغ حجم الملف.

---

### 8️⃣ `src/elt_pipeline.py` (منسق خط البيانات)
* **الدالة `run_pipeline(csv_path, engine_override, threshold_mb, reset_db)`**:
  * تفحص وجود الملف، تهيئ MongoDB، تستشير الموجّه `file_router`.
  * تُنفذ المحرك المختار.
  * تحتوي على ميزة **Automated Fallback**: في حال فشل تشغيل PySpark (بسبب غياب بيئة جافا أو مشكلة في المنفذ)، يقوم النظام تلقائياً بالتحويل الفوري والذكي إلى `python_batch` وإعادة المعالجة بنجاح دون توقف الخط.
  * تسجل النتائج وتطبع ملخص التنفيذ.

---

### 9️⃣ `src/incremental_loader.py` (محرك التحميل التزايدي - المسار B)
* **الدالة `process_incremental_delta(delta_csv_path, db, run_id)`**:
  * تستقبل ملف الـ Delta الذي يحتوي على السجلات الجديدة والمعدلة فقط.
  * تطبق قواعد الجودة والتصنيف وتنفذ `Upsert` على `orders_validated`.
  * تقيس بدقة:
    * `inserted_count`: السجلات الجديدة المضافة فعلياً.
    * `updated_count`: السجلات التي تم تعديل حالتها أو بياناتها.
    * `unchanged_count`: السجلات المتطابقة تماماً.
  * تضمن الـ Idempotency: عند إعادة إرسال نفس ملف الـ Delta، يكون `inserted_count = 0`.

---

### 🔟 `src/analytics_queries.py` (سكربت الاستعلامات والعرض أمام الدكتور)
* **الوظيفة**: ينفذ استعلامات تجميعية متقدمة (`Aggregation Pipelines`) على MongoDB لإظهار النتائج الحية للدكتور:
  1. التحقق من معادلة الاتساق لآخر عملية تشغيل.
  2. إجمالي المبيعات والإيرادات ومتوسط الطلب حسب المدينة (`Sanaa`, `Aden`, `Taiz`, إلخ).
  3. أعلى 5 عملاء إنفاقاً.
  4. تحليل توزيع القواعد التسع التي تم تفعيلها مع عينة لوثيقة مصححة تحتوي `corrections`.
  5. تحليل أسباب العزل مع عينة لوثيقة معزولة.
  6. توزيع طرق الدفع والتوصيل.

---

## 3. قواعد التنظيف التسع والعزل وأثر التصحيح

| # | القاعدة | رمز القاعدة `rule_code` | المثال الأصلي | القيمة المصححة |
|:---:|:---|:---|:---|:---|
| **1** | الأرقام العربية-الهندية | `ARABIC_NUMERALS_NORMALIZED` | `٥٠٠٠` أو `١٢٣` | `5000.0` أو `123.0` |
| **2** | توحيد العملة واستخراج السعر | `CURRENCY_STANDARDIZED` | `5000 ريال` أو `ريال يمني` | `5000.0` و العملة `YER` |
| **3** | فواصل الآلاف | `THOUSANDS_SEPARATOR_REMOVED` | `125,000.00` | `125000.0` |
| **4** | الأرقام المكتوبة نصاً باللغة العربية | `TEXT_NUMBER_CONVERTED` | `ألفان` أو `خمسة آلاف` | `2000.0` أو `5000.0` |
| **5** | تنظيف وتوحيد أرقام الهواتف | `PHONE_CLEANED_DIGITS` | `+967 77 123 4567` أو `077...` | `967771234567` |
| **6** | إصلاح الرموز المكررة في البريد | `EMAIL_REPEATED_SYMBOLS` | `user@@mail..com` | `user@mail.com` |
| **7** | توحيد التواريخ إلى صيغة ISO | `DATE_NORMALIZED_ISO` | `31-01-2025` أو `2025/01/31` | `2025-01-31` |
| **8** | توحيد مرادفات الحالات وطرق الدفع | `STATUS_SYNONYM_MAPPED` | `مؤكد`، `نقدا`، `ملغي` | `CONFIRMED`, `CASH`, `CANCELLED` |
| **9** | مطابقة وإعادة حساب الإجمالي | `TOTAL_RECONCILED` | إجمالي غير متطابق مع العناصر والتوصيل | إعادة حسابه بدقة: $\sum(\text{items}) + \text{delivery}$ |

### رموز العزل في `orders_quarantine`:
* `MISSING_ORDER_ID`: معرف الطلب فارغ أو غير موجود.
* `MISSING_CUSTOMER_ID`: معرف العميل فارغ.
* `INVALID_IMPOSSIBLE_DATE`: تاريخ تالف أو خارج النطاق المنطقي (1990–2030).
* `CORRUPTED_ITEMS_JSON`: نص الـ JSON للعناصر غير قابل للتحليل أو تالف.
* `EMPTY_ITEMS`: قائمة العناصر فارغة بعد التحليل.
* `UNKNOWN_PRICE`: السعر الإجمالي مجهول ولا توجد عناصر لإعادة حسابه.
* `AMBIGUOUS_NEGATIVE_VALUE`: مبالغ مالية سالبة غير مبررة (سعر، توصيل، دفع).
* `MULTIPLE_CONFLICTING_ERRORS`: اجتماع أكثر من خطأ جسيم في نفس السجل.

---

## 4. المسار المتقدم B: التحميل التزايدي (Track B)

### الخطوات الثلاث لإثبات المسار B:
1. **التشغيل الأساسي (Initial Load)**:
   * تحميل العينة الأساسية لتكوين الحالة الابتدائية في `orders_validated`.
2. **تشغيل ملف الـ Delta الأول (`delta_orders.csv`)**:
   * يحتوي على سجلات جديدة (New Orders) + سجلات معدلة (Modified Orders) + سجلات بدون تعديل (Unchanged).
   * النتيجة: زيادة عدد السجلات في `orders_validated` بمقدار السجلات الجديدة فقط (`inserted_count`)، وتحديث السجلات المعدلة (`updated_count`).
3. **إعادة تشغيل نفس ملف الـ Delta مرة ثانية**:
   * النتيجة: `inserted_count = 0`، وبقاء عدد السجلات في `orders_validated` ثابتاً تماماً دون زيادة، مما يثبت الـ **Idempotency** رياضياً وعملياً.

---

## 5. سيناريوهات "ماذا لو؟" (What-If Analysis)

### ❓ ماذا لو قام الدكتور بتمرير ملف بمسار مختلف تماماً؟
* **الجواب**: النظام مهيأ بالكامل لاستقبال أي مسار عبر سطر الأوامر:
  ```bash
  python main.py "D:\Doctor_Folder\new_dataset.csv"
  ```
  يقوم الـ Router بقياس حجم الملف الجديد فوراً واختيار المحرك المناسب ومعالجته وتخزينه دون الحاجة لأي تعديل في الكود.

### ❓ ماذا لو قمنا بتغيير حد الـ Threshold من 200MB إلى 50MB؟
* **الجواب**: يتم تغيير الحد إما من خلال متغير البيئة `SMALL_FILE_THRESHOLD_MB` أو عبر سطر الأوامر:
  ```bash
  python main.py data/sample_orders_small.csv --threshold 30
  ```
  عندها سيقوم الـ Router بتحويل ملف الـ 41MB إلى محرك `PySpark` بدلاً من `Python Batch`، مما يثبت ديناميكية النظام.

### ❓ ماذا لو كان حجم الملف 500 مليون سجل (100 جيجابايت)؟
* **الجواب**:
  * في **Python Batch**: لن تنهار الذاكرة لأننا نستخدم `csv.DictReader` كـ Stream مع دفعات محددة (`BATCH_SIZE=2000`)، وذاكرة الـ RAM تظل ثابتة عند حوالي 80 ميجابايت.
  * في **PySpark**: المعمارية مصممة باستخدام `mapPartitions` مع اتصالات PyMongo مستقلة لكل Executor، وبالتالي لا يتم سحب البيانات الضخمة إلى الـ Driver نهائياً.

### ❓ ماذا لو انقطع الاتصال بقاعدة بيانات MongoDB أثناء المعالجة؟
* **الجواب**: عمليات الدفعات تستخدم `ordered=False` و `UpdateOne(..., upsert=True)`. عند إعادة تشغيل الخط بعد عودة الاتصال، يستأنف النظام العمل دون إنشاء أي سجلات مكررة بفضل مفتاح `order_id` الفريد.

---

## 6. بنك الأسئلة المتوقعة في المناقشة الشفهية

### س1: ما الفرق الجوهري بين ما قمتم به وبين مشاريع الـ ETL التقليدية؟
> **الإجابة النموذجية**: في هذا المشروع طبقنا نمط **ELT الحقيقي**؛ حيث قمنا أولاً بتخزين 100% من السجلات الخام في `orders_raw` ببياناتها الأصلية مع بيانات التتبع (`run_id`, `source_file`, `ingested_at`, `engine_used`) قبل أي عملية تنظيف. هذا يضمن عدم فقدان أي بيانات، ويسمح بإعادة التحليل مستقبلاً إذا تغيرت القواعد. ثم قمنا بعمليات التحويل والتصنيف داخل الذاكرة ونقل السجلات إلى `orders_validated` أو `orders_quarantine`.

### س2: كيف أثبتم الـ Idempotency عملياً؟
> **الإجابة النموذجية**: وضعنا فهارس فريدة (`Unique Index`) على حقل المفتاح التجاري الثابت `order_id` في `orders_validated`، واستخدمنا عمليات `UpdateOne(..., upsert=True)`. وأثبتنا ذلك بتشغيل نفس الملف مرتين متتاليتين؛ في المرة الأولى تم إدخال السجلات، وفي المرة الثانية كان `inserted_count = 0` ولم يزداد عدد وثائق المجموعة على الإطلاق.

### س3: كيف تجنبتم حدوث Out-Of-Memory (OOM) في PySpark؟
> **الإجابة النموذجية**: استخدمنا دالة `mapPartitions`؛ بحيث يقوم كل Executor بإنشاء اتصال مباشر بقاعدة بيانات MongoDB وكتابة القسم الخاص به بالتوازي دون إرسال البيانات المجمعة إلى الـ Driver. الـ Driver يستقبل فقط ملخصاً صغيراً جداً بالعدادات عبر `yield summary_dict`.

### س4: كيف تتحققون من عدم فقدان أي سجل؟
> **الإجابة النموذجية**: نطبق **معادلة الاتساق الأساسية (Section 6.11)** في نهاية كل تشغيل:
> `rows_read == valid_count + corrected_count + quarantine_count`
> ويتم التحقق منها وتسجيلها في تقرير `pipeline_audit_logs.is_consistent`.

---

## 7. دليل أوامر التشغيل ورفع المشروع

### 1. تشغيل الخط على عينة البيانات الصغيرة (Auto Router -> Python Batch):
```bash
python main.py data/sample_orders_small.csv --reset-db
```

### 2. تشغيل الخط على أي ملف مخصص يحدده الدكتور:
```bash
python main.py "مسار_الملف_الجديد.csv"
```

### 3. تشغيل استعلامات المناقشة الشفهية الحية:
```bash
python src/analytics_queries.py
```

### 4. تشغيل عرض المسار المتقدم B (التحميل التزايدي و Idempotency):
```bash
python src/demo_track_b.py
```

### 5. تشغيل جميع اختبارات الوحدة:
```bash
pytest tests/ -v
```

### 6. توليد الرسوم البيانية التفاعلية وعرض النتائج:
```bash
python src/visualize_results.py
```

### 7. رفع المشروع إلى مستودع GitHub الخاص بك:
```bash
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPOSITORY.git
git branch -M main
git push -u origin main
```
*(ملاحظة: تم ضبط `.gitignore` تلقائياً لمنع رفع الملف الضخم 13GB وتجاوز حدود GitHub).*

---

## 8. المقارنة الشاملة: SQL vs NoSQL (MongoDB) vs Apache Spark

| وجه المقارنة | قواعد البيانات العلائقية (SQL / RDBMS) | قواعد بيانات المستندات (NoSQL - MongoDB) | محرك الحوسبة الموزعة (Apache Spark) |
|---|---|---|---|
| **الدور الأساسي (Primary Role)** | تخزين البيانات المنظمة وإجراء العمليات اليومية (OLTP) | تخزين المستندات شبه المنظمة والمرنة (Document Store) | محرك معالجة وتحليل البيانات الضخمة في الذاكرة (Distributed Compute Engine) |
| **نموذج البيانات (Data Model)** | جداول، صفوف، وأعمدة ذات هيكل ثابت (Rigid Schema) | مستندات BSON / JSON مرنة متداخلة (Flexible Schema) | هياكل بيانات موزعة غير قابلة للتغيير (RDDs, DataFrames) |
| **قابلية التوسع (Scalability)** | توسع رأسي (Vertical Scaling) عبر ترقية مواصفات الخادم | توسع أفقي ممتاز (Horizontal Sharding) وسهل التوزيع | توسع أفقي هائل على مئات الخوادم والأنوية المتعددة |
| **لغة الاستعلام (Query Language)** | Structured Query Language (SQL) مع أوامر `JOIN` | MongoDB Query Language (MQL) و `Aggregation Pipelines` | Spark SQL, PySpark DataFrame API, و RDD `mapPartitions` |
| **معالجة البيانات الضخمة (Big Data Processing)** | تختنق عند التعامل مع مئات الجيجابايتات أو الملايين من الصفوف | ممتازة في التخزين والفهرسة ولكن المعالجة الحسابية المعقدة محدودة | الأسرع عالمياً بفضل المعالجة في الذاكرة (In-Memory Computing) |
| **الضمانات الرياضية (Guarantees)** | توافق تام مع معايير ACID الصارمة | توافق ACID على مستوى المستند الفردي مع Idempotency عبر الفهارس الفريدة | خطط تنفيذ معزولة، ومقاومة للأخطاء (Fault-Tolerant via Lineage Graph) |

### 🛠️ كيفية استخدام والتعامل مع MongoDB في المشروع:
1. **رابط الاتصال (Connection URI)**: `mongodb://localhost:27017`
2. **اسم قاعدة البيانات**: `ecommerce_store`
3. **استعراض البيانات عبر MongoDB Compass**:
   * قم بفتح **MongoDB Compass** والضغط على **Connect**.
   * اختر قاعدة بيانات `ecommerce_store` وستشاهد المجموعات الأربع:
     * `orders_raw`: السجلات الأصلية 100% كما وردت من الملف.
     * `orders_validated`: السجلات المصححة والسليمة المعتمدة للتحليل.
     * `orders_quarantine`: السجلات التي تم عزلها مع ذكر سبب العزل في حقل `quarantine_reason`.
     * `pipeline_audit_logs`: ملخص تشغيل خط الأنابيب، معدل السرعة، ومعادلة الاتساق.

---

## 9. الملفات الأربعة الجوهرية في المشروع

إذا سألك الدكتور عن **أهم 4 ملفات**، يمكنك الإجابة من عدة أبعاد معمارية:

### أ) أهم 4 ملفات كودية (Core Python Modules):
1. **`main.py`**: المدخل التنفيذي وضابط الواجهة والترميز العربي لسطر الأوامر.
2. **`src/elt_pipeline.py`**: العقل المدبر لخط الأنابيب الذي يدير التوجيه الذكي ويسجل سجلات التدقيق.
3. **`src/quality_rules.py`**: محرك الجودة والتنظيف الذي ينفذ القواعد التسع والعزل وتتبع التصحيحات (Audit Trail).
4. **`src/spark_loader.py`**: محرك PySpark الموزع الذي يوزع البيانات على 110 Partitions ويكتب لـ MongoDB بالتوازي عبر `mapPartitions`.

### ب) أهم 4 ملفات بيانات (Datasets):
1. **`orders_huge_mixed_quality.csv`** ($12.65\text{ GB}$ / 30.2 مليون سجل): ملف البيانات الضخمة لاختبار Apache Spark والأداء العالي.
2. **`data/sample_orders_small.csv`** ($41.77\text{ MB}$ / 100 ألف سجل): عينة اختبارات الأداء السريعة أمام الدكتور.
3. **`data/delta_orders.csv`** ($533\text{ KB}$): ملف المسار B لإثبات التحديث التزايدي والـ Idempotency.
4. **أي ملف خارجي جديد يمرره الدكتور** لاختبار مرونة وديناميكية النظام.

### ج) المجموعات الأربع في MongoDB:
1. `orders_raw`: وعاء البيانات الخام.
2. `orders_validated`: وعاء البيانات الجاهزة للتحليل التجاري.
3. `orders_quarantine`: مستودع السجلات المعزولة للتدقيق.
4. `pipeline_audit_logs`: سجلات قياس الأداء والاتساق.
