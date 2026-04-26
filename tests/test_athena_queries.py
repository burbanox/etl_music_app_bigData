from chinook_analytics.athena_queries import REPORT_QUERIES


def test_report_queries_cover_required_analytics():
    assert set(REPORT_QUERIES) == {
        "tracks_sold_by_day",
        "top_artist_by_month",
        "best_day_of_week",
        "best_sales_month",
    }


def test_fact_queries_reference_dimensions():
    joined_sql = "\n".join(REPORT_QUERIES.values()).lower()
    assert "fact_sales" in joined_sql
    assert "dim_date" in joined_sql
    assert "dim_track" in joined_sql
    assert "row_number()" in joined_sql
