import sys
from pathlib import Path
import pytest

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.quality_rules import (
    clean_arabic_and_text_number,
    clean_currency_and_amount,
    clean_phone_number,
    clean_email,
    clean_date,
    clean_status_and_synonyms,
    parse_and_validate_items,
)


def test_rule_1_arabic_numerals_normalization():
    corrections = []
    val = clean_arabic_and_text_number("١٢٥٠", "amount", corrections)
    assert val == 1250.0
    assert len(corrections) == 1
    assert corrections[0]["rule_code"] == "ARABIC_NUMERALS_NORMALIZED"


def test_rule_2_currency_standardization():
    corrections = []
    amt, curr = clean_currency_and_amount("5000 ريال", "ريال يمني", corrections)
    assert amt == 5000.0
    assert curr == "YER"
    assert any(c["rule_code"] == "CURRENCY_STANDARDIZED" for c in corrections)


def test_rule_3_thousands_separator():
    corrections = []
    val = clean_arabic_and_text_number("125,000.50", "total_amount", corrections)
    assert val == 125000.50
    assert len(corrections) == 1
    assert corrections[0]["rule_code"] == "THOUSANDS_SEPARATOR_REMOVED"


def test_rule_4_textual_number_words():
    corrections = []
    val = clean_arabic_and_text_number("خمسة آلاف", "total_amount", corrections)
    assert val == 5000.0
    assert len(corrections) == 1
    assert corrections[0]["rule_code"] == "TEXT_NUMBER_CONVERTED"


def test_rule_5_phone_sanitization():
    corrections = []
    phone = clean_phone_number("+967 77 123 4567", corrections)
    assert phone == "967771234567"
    assert len(corrections) == 1
    assert corrections[0]["rule_code"] == "PHONE_CLEANED_DIGITS"


def test_rule_6_email_repeated_symbols():
    corrections = []
    email = clean_email("user@@mail..com", corrections)
    assert email == "user@mail.com"
    assert len(corrections) == 1
    assert corrections[0]["rule_code"] == "EMAIL_REPEATED_SYMBOLS"


def test_rule_7_date_normalization():
    corrections = []
    dt, impossible = clean_date("31-01-2025", corrections)
    assert dt == "2025-01-31"
    assert impossible is False
    assert len(corrections) == 1
    assert corrections[0]["rule_code"] == "DATE_NORMALIZED_ISO"


def test_rule_8_status_synonyms():
    corrections = []
    status = clean_status_and_synonyms("  مؤكد  ", "status", corrections)
    assert status == "CONFIRMED"
    assert len(corrections) == 1
    assert corrections[0]["rule_code"] == "STATUS_SYNONYM_MAPPED"


def test_items_json_parsing():
    items_raw = '[{"sku": "SKU-1", "name": "Phone", "qty": "٢", "unit_price": "500", "total": "1000"}]'
    items, errors = parse_and_validate_items(items_raw)
    assert errors == []
    assert len(items) == 1
    assert items[0]["qty"] == 2
    assert items[0]["unit_price"] == 500.0


def test_rule_9_total_reconciliation():
    """Rule 9: Reconciles total_amount when items sum + delivery cost differs from input."""
    from src.quality_rules import transform_and_classify_row

    raw = {
        "order_id": "ORD-RECON-01",
        "order_date": "2025-01-15",
        "status": "CONFIRMED",
        "customer_id": "CUST-999",
        "customer_name": "Test User",
        "customer_phone": "967771234567",
        "customer_email": "user@example.com",
        "city": "Sanaa",
        "district": "Sabeen",
        "delivery_type": "EXPRESS",
        "delivery_cost": "1000",
        "payment_method": "CASH",
        "payment_status": "PAID",
        "payment_amount": "6000",
        "currency": "YER",
        "total_amount": "99999",  # WRONG AMOUNT in raw data
        "items_json": '[{"sku": "A", "name": "Item A", "qty": 2, "unit_price": 2500, "total": 5000}]',
    }
    cat, val_doc, quar_doc = transform_and_classify_row(raw, run_id="test_run")
    assert cat == "CORRECTED"
    assert val_doc is not None
    assert quar_doc is None
    # Expected: (2 * 2500) + 1000 = 6000.0
    assert val_doc["total_amount"] == 6000.0
    reconciled_corrections = [c for c in val_doc["corrections"] if c["rule_code"] == "TOTAL_RECONCILED"]
    assert len(reconciled_corrections) == 1
    assert reconciled_corrections[0]["original_value"] == 99999.0
    assert reconciled_corrections[0]["corrected_value"] == 6000.0


def test_file_router_small_file():
    """File Router routes files <= threshold to python_batch."""
    from pathlib import Path
    from src.file_router import route_file

    sample_path = Path("data/sample_orders_small.csv")
    if sample_path.exists():
        decision = route_file(sample_path, threshold_mb=200)
        assert decision.engine == "python_batch"
        assert "PYTHON_BATCH" in decision.reason


def test_file_router_engine_override():
    """File Router respects manual engine overrides."""
    from pathlib import Path
    from src.file_router import route_file

    sample_path = Path("data/sample_orders_small.csv")
    if sample_path.exists():
        decision_spark = route_file(sample_path, engine_override="pyspark")
        assert decision_spark.engine == "pyspark"
        assert "Manual override" in decision_spark.reason

        decision_batch = route_file(sample_path, engine_override="python_batch")
        assert decision_batch.engine == "python_batch"

