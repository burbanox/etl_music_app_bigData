CREATE DATABASE IF NOT EXISTS {database};

CREATE EXTERNAL TABLE IF NOT EXISTS {database}.dim_customer (
  CustomerKey int,
  FirstName string,
  LastName string,
  Company string,
  Country string,
  City string,
  State string,
  Email string
)
STORED AS PARQUET
LOCATION 's3://{bucket}/{curated_prefix}/dim_customer/';

CREATE EXTERNAL TABLE IF NOT EXISTS {database}.dim_track (
  TrackKey int,
  Name string,
  Album string,
  Artist string,
  Genre string,
  MediaType string,
  Composer string,
  Milliseconds int
)
STORED AS PARQUET
LOCATION 's3://{bucket}/{curated_prefix}/dim_track/';

CREATE EXTERNAL TABLE IF NOT EXISTS {database}.dim_date (
  DateKey int,
  FullDate string,
  Year int,
  Quarter int,
  Month int,
  Day int,
  DayOfWeek string,
  IsHoliday boolean
)
PARTITIONED BY (partition_year int, partition_month int, partition_day int)
STORED AS PARQUET
LOCATION 's3://{bucket}/{curated_prefix}/dim_date/';

CREATE EXTERNAL TABLE IF NOT EXISTS {database}.fact_sales (
  CustomerKey int,
  TrackKey int,
  InvoiceDateKey int,
  EmployeeKey int,
  Quantity int,
  UnitPrice decimal(10,2),
  TotalAmount decimal(10,2)
)
PARTITIONED BY (partition_year int, partition_month int, partition_day int)
STORED AS PARQUET
LOCATION 's3://{bucket}/{curated_prefix}/fact_sales/';

CREATE EXTERNAL TABLE IF NOT EXISTS {database}.customer_history (
  customer_id int,
  first_name string,
  last_name string,
  company string,
  address string,
  city string,
  state string,
  country string,
  postal_code string,
  phone string,
  fax string,
  email string,
  support_rep_id int
)
PARTITIONED BY (snapshot_date date)
STORED AS PARQUET
LOCATION 's3://{bucket}/{curated_prefix}/customer_history/';

CREATE EXTERNAL TABLE IF NOT EXISTS {database}.employee_reports_history (
  EmployeeKey int,
  FirstName string,
  LastName string,
  Title string,
  ReportsToEmployeeKey int,
  ReportsToName string
)
PARTITIONED BY (snapshot_date date)
STORED AS PARQUET
LOCATION 's3://{bucket}/{curated_prefix}/employee_reports_history/';

MSCK REPAIR TABLE {database}.dim_date;
MSCK REPAIR TABLE {database}.fact_sales;
MSCK REPAIR TABLE {database}.customer_history;
MSCK REPAIR TABLE {database}.employee_reports_history;
