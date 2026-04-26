TRACKS_SOLD_BY_DAY = """
SELECT
  d.FullDate,
  SUM(f.Quantity) AS tracks_sold
FROM fact_sales f
JOIN dim_date d ON f.InvoiceDateKey = d.DateKey
GROUP BY d.FullDate
ORDER BY d.FullDate;
""".strip()

TOP_ARTIST_BY_MONTH = """
WITH artist_month_sales AS (
  SELECT
    d.Year,
    d.Month,
    t.Artist,
    SUM(f.Quantity) AS tracks_sold,
    ROW_NUMBER() OVER (
      PARTITION BY d.Year, d.Month
      ORDER BY SUM(f.Quantity) DESC, t.Artist
    ) AS rn
  FROM fact_sales f
  JOIN dim_date d ON f.InvoiceDateKey = d.DateKey
  JOIN dim_track t ON f.TrackKey = t.TrackKey
  GROUP BY d.Year, d.Month, t.Artist
)
SELECT Year, Month, Artist, tracks_sold
FROM artist_month_sales
WHERE rn = 1
ORDER BY Year, Month;
""".strip()

BEST_DAY_OF_WEEK = """
SELECT
  d.DayOfWeek,
  SUM(f.Quantity) AS tracks_sold,
  COUNT(*) AS sale_lines
FROM fact_sales f
JOIN dim_date d ON f.InvoiceDateKey = d.DateKey
GROUP BY d.DayOfWeek
ORDER BY tracks_sold DESC, sale_lines DESC
LIMIT 1;
""".strip()

BEST_SALES_MONTH = """
SELECT
  d.Year,
  d.Month,
  SUM(f.Quantity) AS tracks_sold,
  SUM(f.TotalAmount) AS revenue
FROM fact_sales f
JOIN dim_date d ON f.InvoiceDateKey = d.DateKey
GROUP BY d.Year, d.Month
ORDER BY tracks_sold DESC, revenue DESC
LIMIT 1;
""".strip()

REPORT_QUERIES = {
    "tracks_sold_by_day": TRACKS_SOLD_BY_DAY,
    "top_artist_by_month": TOP_ARTIST_BY_MONTH,
    "best_day_of_week": BEST_DAY_OF_WEEK,
    "best_sales_month": BEST_SALES_MONTH,
}
