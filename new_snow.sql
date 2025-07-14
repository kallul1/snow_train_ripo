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





   

