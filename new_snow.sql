--database and warehouse
CREATE DATABASE SF_DEMO;

USE DATABASE SF_DEMO;

CREATE SCHEMA SF_DEMO.DEMO;

USE SCHEMA SF_DEMO.DEMO;

CREATE WAREHOUSE DEMO_WH 
  WAREHOUSE_TYPE = STANDARD 
  WAREHOUSE_SIZE = XSMALL ;

---tables
-- Permanent Table This is the default table type. Data persists until explicitly dropped.
CREATE or replace TABLE employee (
    id INT,
    name STRING,
    department STRING,
    hire_date DATE
);

create or replace table sf_demo.demo.clone_customer as
select * from snowflake_sample_data.tpch_sf1.customer;

select * from sf_demo.demo.clone_customer;

--Transient Table : does not support Time Travel or Fail-safe
CREATE TRANSIENT TABLE staging_customers (
    customer_id INT,
    name STRING,
    signup_date DATE
);

--Temporary Table : Data persists only for the session; ideal for intermediate results
CREATE TEMPORARY TABLE temp_sales_summary (
    region STRING,
    total_sales NUMBER
);

-- External Table : References data stored outside Snowflake (e.g., in S3, GCS, or Azure Blob Storage).
CREATE EXTERNAL TABLE ext_sales (
    order_id STRING,
    amount NUMBER,
    order_date DATE
)
WITH LOCATION = @my_external_stage/sales_data/ --location of the file storage path (S3, Blob)
FILE_FORMAT = (TYPE = CSV);

--Dynamic Tables 
CREATE OR REPLACE DYNAMIC TABLE sales_summary
  TARGET_LAG = '5 minutes'  -- Refreshes data with at most 5 minutes lag
  WAREHOUSE = demo_wh
AS
  SELECT
    region,
    product_id,
    SUM(sale_amount) AS total_sales,
    COUNT(*) AS order_count
  FROM raw_sales_data
  GROUP BY region, product_id;

--Iceberg Tables
--we need to create and configure Iceberg catalog using Snowflake Native Catalog or an external catalog (like AWS Glue).
CREATE ICEBERG TABLE my_catalog.db.iceberg_sales (
    sale_id STRING,
    region STRING,
    product STRING,
    amount NUMBER
);

CREATE ICEBERG TABLE my_catalog.db.external_iceberg_table
  EXTERNAL_VOLUME = my_s3_external_volume
  TABLE_FORMAT = 'ICEBERG'
  LOCATION = 's3://my-data-lake/iceberg_sales/'
  AS
  SELECT * FROM staging_sales_data;

---views
CREATE OR REPLACE VIEW orders_details AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS;

--MATERIALIZED VIEW
CREATE OR REPLACE MATERIALIZED VIEW supplier_details AS
SELECT
  s_suppkey,
  s_name,
  s_address,
  s_phone
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.SUPPLIER
;

--SECURE VIEW
CREATE OR REPLACE SECURE VIEW secure_customer_view AS
SELECT
  c_custkey,
  c_name,
  c_phone
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER;

SELECT * FROM  SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.SUPPLIER LIMIT 100;

---stage
CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/myrole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://my-bucket/data/');

CREATE OR REPLACE STAGE my_s3_stage
  URL = 's3://my-bucket/data/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"');


----procedure
CREATE OR REPLACE PROCEDURE insert_audit_log(
    action STRING,
    username STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  INSERT INTO audit_log(action, performed_by, performed_at)
  VALUES (:action, :username, CURRENT_TIMESTAMP);
  
  RETURN 'Audit log inserted';
END;
$$;


CALL insert_audit_log('DELETE_CUSTOMER', 'admin_user');

--function
CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
  SPLIT_PART(email, '@', 1) 
$$;

select mask_email('user_name@mail.com');


--table function
CREATE OR REPLACE FUNCTION get_order_details(n INT)
RETURNS TABLE (orderkey INT, partkey int, shipdate date, commitdate date)
AS
$$
  SELECT l_orderkey,  l_partkey, l_shipdate, l_commitdate
  FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.LINEITEM 
  WHERE l_orderkey = n
  ORDER BY l_linenumber
  limit 10
  
$$;

select * from table(get_order_details(510484615));
--510484615
--510442948

--sequence
CREATE OR REPLACE SEQUENCE order_id_seq
  START WITH = 1000
  INCREMENT = 1
  ORDER --NOORDER
;

SELECT order_id_seq.nextval;


--pipe
CREATE OR REPLACE PIPE sales_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO raw_sales
  FROM @my_stage
  FILE_FORMAT = (FORMAT_NAME = my_csv_format);


--streams
CREATE OR REPLACE STREAM employee_changes_stream
  ON TABLE employee
  APPEND_ONLY = false 
  SHOW_INITIAL_ROWS = false;


  insert into employee values
  (1,'aaa','hr','2000-10-11');

  select * from employee_changes_stream;

  insert into employee values
  (2,'bbb','sales','2004-01-07'),
  (3,'ccc','production','2008-02-19');

  update employee set hire_date = '2008-02-15' where id = 3;


select * from employee_changes_stream;
  

---TASK
CREATE OR REPLACE TASK demo_task
  WAREHOUSE = compute_wh
  SCHEDULE = '10 SECONDS'
AS
  INSERT INTO employee
  SELECT order_id_seq.nextval,'DEMO', 'TEST', CURRENT_DATE, CURRENT_TIMESTAMP;

ALTER TABLE employee ADD COLUMN INSERT_TIME TIMESTAMP;  
select * from employee;

alter task demo_task resume;
alter task demo_task suspend;


create table employee_scd 
(id int,name varchar, status varchar);

CREATE OR REPLACE TASK process_employee_changes
  WAREHOUSE = compute_wh
  SCHEDULE = 'USING CRON */10 * * * * UTC'  -- every hour
AS
  MERGE INTO employee_scd t
  USING (
    SELECT * FROM employee_changes_stream
  ) s
  ON t.id = s.id
  WHEN MATCHED AND s.METADATA$ACTION = 'UPDATE' THEN
    UPDATE SET name = s.name, status = s.METADATA$ACTION
  WHEN NOT MATCHED THEN
    INSERT (id, name, status) VALUES (s.id, s.name, s.METADATA$ACTION);

alter task process_employee_changes resume    ;

  select * from employee_scd;
  truncate table employee_scd;