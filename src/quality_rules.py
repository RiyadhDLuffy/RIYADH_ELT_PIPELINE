"""Automated data quality rules, cleaning transformations, audit trail, and quarantine classification."""
from __future__ import annotations

import hashlib
import json
import math
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    ARABIC_DIGITS_TABLE,
    ARABIC_WORDS_TO_NUMBERS,
    STATUS_SYNONYMS,
)

# ---------------------------------------------------------------------------
# Pre-compiled regex patterns (compiled once at import, not per-row)
# ---------------------------------------------------------------------------
_RE_NON_NUMERIC = re.compile(r"[^\d.-]")
_RE_AT = re.compile(r"@+")
_RE_DOT = re.compile(r"\.+")
_RE_NON_DIGIT = re.compile(r"\D")



def utc_now() -> str:
    """Returns current UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def as_text(value: Any) -> str:
    """Converts a raw value to a trimmed string; handles None, NaN, and null equivalents."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "null", "undefined"} else text


def generate_issue_key(order_id: str | None, raw_record: dict[str, Any]) -> str:
    """Generates a stable unique issue key for quarantined records."""
    if order_id:
        return f"order:{order_id}"
    stable_repr = json.dumps(raw_record, ensure_ascii=False, sort_keys=True, default=str)
    return "row:" + hashlib.sha256(stable_repr.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Individual Transformation Rules with Correction Recording
# ---------------------------------------------------------------------------

def clean_arabic_and_text_number(
    raw_val: Any,
    field_name: str,
    corrections: list[dict[str, Any]],
) -> float | None:
    """Rule 1, 3, 4: Converts Arabic digits, removes grouping commas, and converts Arabic number words."""
    orig_str = as_text(raw_val)
    if not orig_str:
        return None

    working = orig_str.strip()

    # Rule 4: Textual number words in Arabic
    if working in ARABIC_WORDS_TO_NUMBERS:
        val = float(ARABIC_WORDS_TO_NUMBERS[working])
        corrections.append({
            "field": field_name,
            "original_value": orig_str,
            "corrected_value": val,
            "rule_code": "TEXT_NUMBER_CONVERTED",
        })
        return val

    # Rule 1: Arabic-Indic numerals translation
    translated = working.translate(ARABIC_DIGITS_TABLE)
    had_arabic_digits = translated != working
    working = translated.replace(" ", "")

    # Rule 3: Thousands separator vs decimal point
    had_comma = "," in working
    if working.count(",") == 1 and "." not in working:
        left, right = working.split(",")
        working = left + ("." + right if len(right) != 3 else right)
    else:
        working = working.replace(",", "")

    # Remove any non-numeric suffixes (e.g. YER, ريال)
    cleaned_digits = _RE_NON_NUMERIC.sub("", working)
    try:
        val = float(cleaned_digits)
        if not math.isfinite(val):
            return None

        if had_arabic_digits:
            corrections.append({
                "field": field_name,
                "original_value": orig_str,
                "corrected_value": val,
                "rule_code": "ARABIC_NUMERALS_NORMALIZED",
            })
        elif had_comma:
            corrections.append({
                "field": field_name,
                "original_value": orig_str,
                "corrected_value": val,
                "rule_code": "THOUSANDS_SEPARATOR_REMOVED",
            })
        return val
    except (ValueError, TypeError):
        return None


def clean_currency_and_amount(
    amount_val: Any,
    currency_val: Any,
    corrections: list[dict[str, Any]],
) -> tuple[float | None, str]:
    """Rule 2: Removes textual currency names from amount and standardizes currency to YER."""
    orig_amt_str = as_text(amount_val)
    orig_curr_str = as_text(currency_val)

    # Detect currency embedded inside amount string
    currency = "YER"
    if any(c in orig_amt_str for c in ["ريال", "YER", "YR", "ر.ي"]):
        corrections.append({
            "field": "currency",
            "original_value": f"amount: '{orig_amt_str}', curr: '{orig_curr_str}'",
            "corrected_value": "YER",
            "rule_code": "CURRENCY_STANDARDIZED",
        })
    elif orig_curr_str:
        norm_curr = orig_curr_str.upper().strip()
        if norm_curr in {"ريال", "ريال يمني", "ر.ي", "YR", "YER", "YEMENI RIAL"}:
            if norm_curr != "YER":
                corrections.append({
                    "field": "currency",
                    "original_value": orig_curr_str,
                    "corrected_value": "YER",
                    "rule_code": "CURRENCY_STANDARDIZED",
                })
            currency = "YER"
        else:
            currency = norm_curr

    num = clean_arabic_and_text_number(orig_amt_str, "amount", corrections)
    return num, currency


def clean_phone_number(
    phone_val: Any,
    corrections: list[dict[str, Any]],
) -> str:
    """Rule 5: Sanitizes spaces, dashes, country code +967 to clean digits."""
    orig_str = as_text(phone_val)
    if not orig_str:
        return ""

    translated = orig_str.translate(ARABIC_DIGITS_TABLE)
    digits = _RE_NON_DIGIT.sub("", translated)

    # Standardize Yemeni 967 prefix or local 7XXXXXXXX
    if digits.startswith("00967"):
        digits = "967" + digits[5:]
    elif digits.startswith("0") and len(digits) == 10:
        digits = "967" + digits[1:]
    elif len(digits) == 9 and digits.startswith("7"):
        digits = "967" + digits

    if digits != orig_str:
        corrections.append({
            "field": "customer_phone",
            "original_value": orig_str,
            "corrected_value": digits,
            "rule_code": "PHONE_CLEANED_DIGITS",
        })
    return digits


def clean_email(
    email_val: Any,
    corrections: list[dict[str, Any]],
) -> str:
    """Rule 6: Fixes duplicate symbols like @@ or .. in email addresses."""
    orig_str = as_text(email_val)
    if not orig_str:
        return ""

    fixed = orig_str.strip().lower()
    fixed = _RE_AT.sub("@", fixed)
    fixed = _RE_DOT.sub(".", fixed)

    if fixed != orig_str:
        corrections.append({
            "field": "customer_email",
            "original_value": orig_str,
            "corrected_value": fixed,
            "rule_code": "EMAIL_REPEATED_SYMBOLS",
        })
    return fixed


def clean_date(
    date_val: Any,
    corrections: list[dict[str, Any]],
) -> tuple[str | None, bool]:
    """Rule 7: Standardizes date formats (YYYY-MM-DD, DD-MM-YYYY, YYYY/MM/DD) to ISO format."""
    orig_str = as_text(date_val)
    if not orig_str:
        return None, False

    translated = orig_str.translate(ARABIC_DIGITS_TABLE).strip()

    iso_result: str | None = None
    for fmt in (
        "%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y",
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            dt = datetime.strptime(translated, fmt)
            if 1990 <= dt.year <= 2030:
                iso_result = dt.date().isoformat()
                break
        except ValueError:
            continue

    if not iso_result:
        try:
            from dateutil.parser import parse
            dt = parse(translated, dayfirst=False, fuzzy=False)
            if 1990 <= dt.year <= 2030:
                iso_result = dt.date().isoformat()
        except Exception:
            pass

    if iso_result is None:
        return None, True  # Impossible/unparseable date

    if iso_result != orig_str:
        corrections.append({
            "field": "order_date",
            "original_value": orig_str,
            "corrected_value": iso_result,
            "rule_code": "DATE_NORMALIZED_ISO",
        })
    return iso_result, False


def clean_status_and_synonyms(
    status_val: Any,
    field_name: str,
    corrections: list[dict[str, Any]],
) -> str:
    """Rule 8: Trims whitespace and standardizes Arabic/English synonyms."""
    orig_str = as_text(status_val)
    if not orig_str:
        return "UNKNOWN"

    trimmed = orig_str.strip()
    norm = STATUS_SYNONYMS.get(trimmed, trimmed.upper())

    if norm != orig_str:
        corrections.append({
            "field": field_name,
            "original_value": orig_str,
            "corrected_value": norm,
            "rule_code": "STATUS_SYNONYM_MAPPED",
        })
    return norm


def parse_and_validate_items(
    items_raw: Any,
) -> tuple[list[dict[str, Any]] | None, list[str]]:
    """Validates and parses items JSON field."""
    if isinstance(items_raw, list):
        items_list = items_raw
    else:
        text = as_text(items_raw)
        if not text:
            return None, ["EMPTY_ITEMS"]
        try:
            items_list = json.loads(text)
        except Exception:
            return None, ["CORRUPTED_ITEMS_JSON"]

    if not isinstance(items_list, list) or len(items_list) == 0:
        return None, ["EMPTY_ITEMS"]

    clean_items: list[dict[str, Any]] = []
    for item in items_list:
        if not isinstance(item, dict):
            return None, ["CORRUPTED_ITEMS_JSON"]

        sku = as_text(item.get("sku"))
        name = as_text(item.get("name"))
        qty_num = clean_arabic_and_text_number(item.get("qty"), "qty", [])
        price_num = clean_arabic_and_text_number(item.get("unit_price"), "unit_price", [])

        qty = int(qty_num) if qty_num is not None else 1
        unit_price = float(price_num) if price_num is not None else 0.0
        total = qty * unit_price if (item.get("total") is None) else (clean_arabic_and_text_number(item.get("total"), "total", []) or (qty * unit_price))

        clean_items.append({
            "sku": sku,
            "name": name,
            "qty": qty,
            "unit_price": unit_price,
            "total": total,
        })

    return clean_items, []


# ---------------------------------------------------------------------------
# Master Classification & Transformation Function
# ---------------------------------------------------------------------------

def transform_and_classify_row(
    raw: dict[str, Any],
    run_id: str,
) -> tuple[str, dict[str, Any] | None, dict[str, Any] | None]:
    """Applies all 9+ quality rules, records the audit trail, and classifies into:
    
    Returns:
        (category, validated_doc, quarantine_doc)
        where category is 'VALID', 'CORRECTED', or 'QUARANTINE'.
    """
    corrections: list[dict[str, Any]] = []
    errors: list[str] = []

    # 1. Identity validation
    order_id = as_text(raw.get("order_id") if "order_id" in raw else raw.get("\ufefforder_id"))
    customer_id = as_text(raw.get("customer_id"))

    if not order_id:
        errors.append("MISSING_ORDER_ID")
    if not customer_id:
        errors.append("MISSING_CUSTOMER_ID")

    # 2. Date validation
    order_date, is_impossible_date = clean_date(raw.get("order_date"), corrections)
    if is_impossible_date or not order_date:
        errors.append("INVALID_IMPOSSIBLE_DATE")

    # 3. Items validation
    items, item_errors = parse_and_validate_items(raw.get("items_json"))
    if item_errors:
        errors.extend(item_errors)

    # 4. Numerics & Negative value checks
    delivery_cost = clean_arabic_and_text_number(raw.get("delivery_cost"), "delivery_cost", corrections) or 0.0
    payment_amt, currency = clean_currency_and_amount(raw.get("payment_amount"), raw.get("currency"), corrections)
    payment_amt = payment_amt or 0.0
    total_amt = clean_arabic_and_text_number(raw.get("total_amount"), "total_amount", corrections)

    if delivery_cost < 0 or payment_amt < 0 or (total_amt is not None and total_amt < 0):
        errors.append("AMBIGUOUS_NEGATIVE_VALUE")

    # 5. Rule 9: Reconcile Total Amount
    if items and not errors:
        items_sum = sum(i["total"] for i in items)
        calculated_total = items_sum + delivery_cost
        if total_amt is None or abs(total_amt - calculated_total) > 0.01:
            if total_amt is not None:
                corrections.append({
                    "field": "total_amount",
                    "original_value": total_amt,
                    "corrected_value": calculated_total,
                    "rule_code": "TOTAL_RECONCILED",
                })
            total_amt = calculated_total

    # Check for unresolvable missing price
    if total_amt is None and not items:
        errors.append("UNKNOWN_PRICE")

    # 6. Customer & Status Fields
    customer_phone = clean_phone_number(raw.get("customer_phone"), corrections)
    customer_email = clean_email(raw.get("customer_email"), corrections)
    order_status = clean_status_and_synonyms(raw.get("status"), "status", corrections)
    payment_status = clean_status_and_synonyms(raw.get("payment_status"), "payment_status", corrections)
    delivery_type = clean_status_and_synonyms(raw.get("delivery_type"), "delivery_type", corrections)
    payment_method = clean_status_and_synonyms(raw.get("payment_method"), "payment_method", corrections)

    # Classify as Quarantine if any fatal errors occur
    if errors:
        if len(errors) > 1:
            errors.append("MULTIPLE_CONFLICTING_ERRORS")
        issue_key = generate_issue_key(order_id, raw)
        quarantine_doc = {
            "issue_key": issue_key,
            "order_id": order_id or None,
            "customer_id": customer_id or None,
            "error_codes": errors,
            "error_details": ", ".join(errors),
            "raw_record": {k: as_text(v) for k, v in raw.items()},
            "last_run_id": run_id,
            "ingested_at": utc_now(),
        }
        return "QUARANTINE", None, quarantine_doc

    # Validated Document
    is_corrected = len(corrections) > 0
    _now = utc_now()
    validated_doc = {
        "order_id": order_id,
        "order_date": order_date,
        "status": order_status,
        "customer": {
            "customer_id": customer_id,
            "name": as_text(raw.get("customer_name")),
            "phone": customer_phone,
            "email": customer_email,
            "address": {
                "city": as_text(raw.get("city")),
                "district": as_text(raw.get("district")),
            },
        },
        "items": items,
        "delivery": {
            "type": delivery_type,
            "cost": delivery_cost,
        },
        "payment": {
            "method": payment_method,
            "status": payment_status,
            "amount": payment_amt,
            "currency": currency,
        },
        "total_amount": total_amt,
        "quality_status": "corrected" if is_corrected else "valid",
        "corrections": corrections,
        "last_run_id": run_id,
        "updated_at": _now,
    }
    return ("CORRECTED" if is_corrected else "VALID"), validated_doc, None
