"""Centralized configuration and environment settings for the ELT pipeline."""
import os
from pathlib import Path

# Base Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "reports"
DOCS_DIR = PROJECT_ROOT / "docs"

# Ensure runtime directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
(REPORTS_DIR / "screenshots").mkdir(parents=True, exist_ok=True)

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "ecommerce_store")

# MongoDB Collections (Official Project Specification)
RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "orders_quarantine"
AUDIT_COLLECTION = "pipeline_audit_logs"

# Engine Threshold (in Megabytes - files <= 200 MB use Python Batch, > 200 MB use PySpark)
SMALL_FILE_THRESHOLD_MB = float(os.getenv("SMALL_FILE_THRESHOLD_MB", "200"))
THRESHOLD_BYTES = int(SMALL_FILE_THRESHOLD_MB * 1024 * 1024)

# Batch & Partition Tuning
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10000"))
SPARK_DEFAULT_PARTITIONS = int(os.getenv("SPARK_PARTITIONS", "8"))
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "8g")

# Normalization & Cleaning Constants
ARABIC_DIGITS_TABLE = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹٫٬", "01234567890123456789.,")

ARABIC_WORDS_TO_NUMBERS = {
    "واحد": 1,
    "اثنان": 2,
    "اثنين": 2,
    "ثلاثة": 3,
    "ثلاث": 3,
    "اربعة": 4,
    "أربعة": 4,
    "خمسة": 5,
    "ستة": 6,
    "سبعة": 7,
    "ثمانية": 8,
    "تسعة": 9,
    "عشرة": 10,
    "مائة": 100,
    "مئة": 100,
    "مائتان": 200,
    "مئتان": 200,
    "ألف": 1000,
    "الف": 1000,
    "ألفان": 2000,
    "الفان": 2000,
    "ألفين": 2000,
    "ثلاثة آلاف": 3000,
    "خمسة آلاف": 5000,
    "خمسة الاف": 5000,
    "عشرة آلاف": 10000,
}

STATUS_SYNONYMS = {
    "مؤكد": "CONFIRMED",
    "مكتمل": "COMPLETED",
    "ملغي": "CANCELLED",
    "ملغى": "CANCELLED",
    "قيد الانتظار": "PENDING",
    "معلق": "PENDING",
    "مدفوع": "PAID",
    "غير مدفوع": "UNPAID",
    "مسترجع": "REFUNDED",
    "مسترد": "REFUNDED",
    "تم الشحن": "SHIPPED",
    "تم التوصيل": "DELIVERED",
    "نقدا": "CASH",
    "نقد": "CASH",
    "بطاقة": "CARD",
    "تحويل": "TRANSFER",
}

RESULTS_JSON_PATH = REPORTS_DIR / "results.json"
RESULTS_MD_PATH = REPORTS_DIR / "results.md"
