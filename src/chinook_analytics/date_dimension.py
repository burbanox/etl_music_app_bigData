from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterable

import holidays


@dataclass(frozen=True)
class DateRow:
    date_key: int
    full_date: date
    year: int
    quarter: int
    month: int
    day: int
    day_of_week: str
    is_holiday: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "DateKey": self.date_key,
            "FullDate": self.full_date.isoformat(),
            "Year": self.year,
            "Quarter": self.quarter,
            "Month": self.month,
            "Day": self.day,
            "DayOfWeek": self.day_of_week,
            "IsHoliday": self.is_holiday,
            "partition_year": self.year,
            "partition_month": self.month,
            "partition_day": self.day,
        }


def to_date_key(value: date | datetime) -> int:
    current_date = value.date() if isinstance(value, datetime) else value
    return int(current_date.strftime("%Y%m%d"))


def iter_dates(start_date: date, end_date: date) -> Iterable[date]:
    if end_date < start_date:
        raise ValueError("end_date must be greater than or equal to start_date")

    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def build_date_dimension(
    start_date: date,
    end_date: date,
    country: str = "CO",
) -> list[DateRow]:
    holiday_calendar = holidays.country_holidays(country, years=range(start_date.year, end_date.year + 1))
    rows: list[DateRow] = []

    for current in iter_dates(start_date, end_date):
        rows.append(
            DateRow(
                date_key=to_date_key(current),
                full_date=current,
                year=current.year,
                quarter=((current.month - 1) // 3) + 1,
                month=current.month,
                day=current.day,
                day_of_week=current.strftime("%A"),
                is_holiday=current in holiday_calendar,
            )
        )

    return rows
