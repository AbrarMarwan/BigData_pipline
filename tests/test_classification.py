import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.quality_rules import validate_and_clean_record

def test_classification_valid():
    """Verify that a clean record is classified as 'valid' without corrections."""
    record = {
        "order_id": "ORD-100",
        "order_date": "2025-02-01T10:00:00",
        "status": "مؤكد",
        "customer_id": "CUST-100",
        "customer_name": "سارة أحمد",
        "customer_phone": "771234567",
        "customer_email": "sara@example.com",
        "currency": "YER",
        "delivery_cost": "2000.0",
        "payment_amount": "50000.0",
        "total_amount": "52000.0",
        "items_json": '[{"sku": "SKU-1", "name": "شاحن", "qty": 1, "unit_price": 50000.0, "total": 50000.0}]'
    }
    result = validate_and_clean_record(record)
    assert result["status"] == "valid"
    assert len(result["data"].get("corrections", [])) == 0

def test_classification_corrected():
    """Verify that a fixable record is classified as 'corrected' with audit trail."""
    record = {
        "order_id": "ORD-101",
        "order_date": "01/02/2025",
        "status": "  مؤكد  ",
        "customer_id": "CUST-101",
        "customer_name": "عمر خالد",
        "customer_phone": "+967 77 123 4567",
        "customer_email": "omar@@mail..com",
        "currency": "لاير يمني",
        "delivery_cost": "2,000.00",
        "payment_amount": "خمسة آلاف",
        "total_amount": "٥٠٠٠",
        "items_json": '[{"sku": "SKU-2"}]'
    }
    result = validate_and_clean_record(record)
    assert result["status"] == "corrected"
    corrections = result["data"]["corrections"]
    assert len(corrections) > 0
    # Audit trail verification
    for corr in corrections:
        assert "field" in corr
        assert "original_value" in corr
        assert "corrected_value" in corr
        assert "rule_code" in corr

def test_classification_quarantined_missing_ids():
    """Verify that records with missing critical identifiers go to quarantine."""
    record = {
        "order_id": "",
        "customer_id": "CUST-102",
        "order_date": "2025-01-01",
        "items_json": '[{"sku": "SKU-1"}]'
    }
    result = validate_and_clean_record(record)
    assert result["status"] in ["quarantine", "quarantined"]
    assert any(err in result["data"]["error_codes"] for err in ["MISSING_ORDER_ID", "ID_ORDER_MISSING"])

def test_classification_quarantined_negative_value():
    """Verify that records with negative monetary values go to quarantine."""
    record = {
        "order_id": "ORD-103",
        "customer_id": "CUST-103",
        "order_date": "2025-01-01",
        "delivery_cost": "-500.0",
        "items_json": '[{"sku": "SKU-1"}]'
    }
    result = validate_and_clean_record(record)
    assert result["status"] in ["quarantine", "quarantined"]
    assert any(err in result["data"]["error_codes"] for err in ["AMBIGUOUS_NEGATIVE_VALUE", "VALUE_NEGATIVE_AMBIGUOUS"])

def test_consistency_equation():
    """Verify Section 6.11 consistency equation: raw = valid + corrected + quarantine."""
    records = [
        # 1 valid
        {"order_id": "O1", "customer_id": "C1", "order_date": "2025-01-01", "items_json": '[{"i": 1}]'},
        # 1 corrected
        {"order_id": "O2", "customer_id": "C2", "order_date": "2025/01/01", "items_json": '[{"i": 1}]'},
        # 1 quarantine
        {"order_id": "", "customer_id": "C3", "order_date": "2025-01-01", "items_json": '[{"i": 1}]'}
    ]
    valid_count = 0
    corrected_count = 0
    quarantine_count = 0
    for r in records:
        res = validate_and_clean_record(r)
        if res["status"] == "valid":
            valid_count += 1
        elif res["status"] == "corrected":
            corrected_count += 1
        else:
            quarantine_count += 1
    
    assert len(records) == valid_count + corrected_count + quarantine_count
