from datetime import date, datetime

from chinook_analytics.date_dimension import build_date_dimension, to_date_key


def test_to_date_key_supports_date_and_datetime():
    assert to_date_key(date(2026, 4, 26)) == 20260426
    assert to_date_key(datetime(2026, 4, 26, 10, 30)) == 20260426


def test_build_date_dimension_has_required_fields_and_partitions():
    rows = build_date_dimension(date(2026, 1, 1), date(2026, 1, 2), country="CO")

    assert len(rows) == 2
    first = rows[0].as_dict()
    assert first["DateKey"] == 20260101
    assert first["FullDate"] == "2026-01-01"
    assert first["Year"] == 2026
    assert first["Quarter"] == 1
    assert first["Month"] == 1
    assert first["Day"] == 1
    assert first["partition_year"] == 2026
    assert first["partition_month"] == 1
    assert first["partition_day"] == 1
    assert first["IsHoliday"] is True
