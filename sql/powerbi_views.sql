CREATE OR REPLACE VIEW chinook_analytics.v_tracks_sold_by_day AS
SELECT
  d.FullDate,
  SUM(f.Quantity) AS TracksSold
FROM chinook_analytics.fact_sales f
JOIN chinook_analytics.dim_date d ON f.InvoiceDateKey = d.DateKey
GROUP BY d.FullDate;

CREATE OR REPLACE VIEW chinook_analytics.v_top_artist_by_month AS
WITH artist_month_sales AS (
  SELECT
    d.Year,
    d.Month,
    t.Artist,
    SUM(f.Quantity) AS TracksSold,
    ROW_NUMBER() OVER (
      PARTITION BY d.Year, d.Month
      ORDER BY SUM(f.Quantity) DESC, t.Artist
    ) AS rn
  FROM chinook_analytics.fact_sales f
  JOIN chinook_analytics.dim_date d ON f.InvoiceDateKey = d.DateKey
  JOIN chinook_analytics.dim_track t ON f.TrackKey = t.TrackKey
  GROUP BY d.Year, d.Month, t.Artist
)
SELECT Year, Month, Artist, TracksSold
FROM artist_month_sales
WHERE rn = 1;

CREATE OR REPLACE VIEW chinook_analytics.v_best_day_of_week AS
SELECT
  d.DayOfWeek,
  SUM(f.Quantity) AS TracksSold,
  COUNT(*) AS SaleLines
FROM chinook_analytics.fact_sales f
JOIN chinook_analytics.dim_date d ON f.InvoiceDateKey = d.DateKey
GROUP BY d.DayOfWeek;

CREATE OR REPLACE VIEW chinook_analytics.v_best_sales_month AS
SELECT
  d.Year,
  d.Month,
  SUM(f.Quantity) AS TracksSold,
  SUM(f.TotalAmount) AS Revenue
FROM chinook_analytics.fact_sales f
JOIN chinook_analytics.dim_date d ON f.InvoiceDateKey = d.DateKey
GROUP BY d.Year, d.Month;
