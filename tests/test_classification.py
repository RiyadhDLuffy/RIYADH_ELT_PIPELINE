import sys
from pathlib import Path
import pytest

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.quality_rules import transform_and_classify_row


def test_classify_valid_row():
    raw = {
        "order_id": "ORD-1001",
        "order_date": "2025-01-15",
        "status": "CONFIRMED",
        "customer_id": "CUST-500",
        "customer_name": "Ali Saleh",
        "customer_phone": "967771234567",
        "customer_email": "ali@example.com",
        "city": "Sanaa",
        "district": "Al-Sabeen",
        "delivery_type": "EXPRESS",
        "delivery_cost": "500",
        "payment_method": "CASH",
        "payment_status": "PAID",
        "payment_amount": "2500",
        "currency": "YER",
        "total_amount": "2500",
        "items_json": '[{"sku": "A1", "name": "Item 1", "qty": 1, "unit_price": 2000, "total": 2000}]',
    }
    cat, val_doc, quar_doc = transform_and_classify_row(raw, run_id="test_run")
    assert cat == "VALID"
    assert val_doc is not None
    assert quar_doc is None
    assert val_doc["order_id"] == "ORD-1001"
    assert val_doc["quality_status"] == "valid"
    assert len(val_doc["corrections"]) == 0


def test_classify_corrected_row():
    raw = {
        "order_id": "ORD-1002",
        "order_date": "15/01/2025",
        "status": "  مؤكد  ",
        "customer_id": "CUST-501",
        "customer_name": "Fatima",
        "customer_phone": "+967 77 999 8888",
        "customer_email": "user@@mail..com",
        "city": "Aden",
        "district": "Crater",
        "delivery_type": "STANDARD",
        "delivery_cost": "٥٠٠",
        "payment_method": "نقدا",
        "payment_status": "مدفوع",
        "payment_amount": "٥٠٠٠ ريال",
        "currency": "ريال يمني",
        "total_amount": "5,500.00",
        "items_json": '[{"sku": "B1", "name": "Item 2", "qty": "1", "unit_price": "5000", "total": "5000"}]',
    }
    cat, val_doc, quar_doc = transform_and_classify_row(raw, run_id="test_run")
    assert cat == "CORRECTED"
    assert val_doc is not None
    assert quar_doc is None
    assert val_doc["order_id"] == "ORD-1002"
    assert val_doc["quality_status"] == "corrected"
    assert len(val_doc["corrections"]) > 0
    assert val_doc["customer"]["phone"] == "967779998888"
    assert val_doc["customer"]["email"] == "user@mail.com"
    assert val_doc["status"] == "CONFIRMED"
    assert val_doc["payment"]["currency"] == "YER"


def test_classify_quarantine_missing_order_id():
    raw = {
        "order_id": "",
        "customer_id": "CUST-502",
        "order_date": "2025-01-15",
        "items_json": '[{"sku": "A1", "name": "Item", "qty": 1, "unit_price": 100, "total": 100}]',
    }
    cat, val_doc, quar_doc = transform_and_classify_row(raw, run_id="test_run")
    assert cat == "QUARANTINE"
    assert val_doc is None
    assert quar_doc is not None
    assert "MISSING_ORDER_ID" in quar_doc["error_codes"]
    assert quar_doc["issue_key"].startswith("row:")


def test_classify_quarantine_corrupted_items():
    raw = {
        "order_id": "ORD-1003",
        "customer_id": "CUST-503",
        "order_date": "2025-01-15",
        "items_json": "INVALID_BROKEN_JSON{",
    }
    cat, val_doc, quar_doc = transform_and_classify_row(raw, run_id="test_run")
    assert cat == "QUARANTINE"
    assert "CORRUPTED_ITEMS_JSON" in quar_doc["error_codes"]


def test_quarantine_negative_values():
    """Quarantine detects ambiguous negative monetary amounts."""
    raw = {
        "order_id": "ORD-NEG-01",
        "order_date": "2025-01-15",
        "customer_id": "CUST-999",
        "delivery_cost": "-500",  # INVALID NEGATIVE
        "payment_amount": "1000",
        "items_json": '[{"sku": "A", "name": "Item A", "qty": 1, "unit_price": 1000, "total": 1000}]',
    }
    cat, val_doc, quar_doc = transform_and_classify_row(raw, run_id="test_run")
    assert cat == "QUARANTINE"
    assert val_doc is None
    assert quar_doc is not None
    assert "AMBIGUOUS_NEGATIVE_VALUE" in quar_doc["error_codes"]

