/*
BRONZE LAYER
	Bronze Layer: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database-
STEP 1 - Analysing
	1.1 Interview Source System Experts ; Ask questions on nature of source systems we are trying to connect
    1.2 Business Context and Ownership :  Understand the story behind the data
		Who Owns the data? : who is responsible for it (departments, etc.),
        Business processes it supports (customer transactions, supply chain, finance, etc.) This helps to understand the importance of the data.
	1.3 System and Data Documentation: Documentation acts as learning material about the data, saving time when designing new data models.
    1.4 Data Model & Data Catalogue : Having descriptions of the columns and tables, and a data catalog helps in understanding how to join tables in the data warehouse.
    1.5 Architecture and Technology Stack : 
		1.5.1 How is Data Stored ? - (on-premise like SQL Server or Oracle, or in the cloud like Azure or AWS).
        1.5.2 What are the integration capabilities?i.e., how to get the data. Does the source system offer APIs or file extractions, or a direct database connection
	1.6 Extract & Load :
		1.6.1 Incremental or Full Load
        1.6.2 Data Scope and Historical Needs : Determine if all data is needed, or only a subset (e.g., 10 years of data). Also, find out if histories are already in the source system or need to be built in the data warehouse.
        1.6.3 Expected Size of Extracts: Determine if you're dealing with megabytes, gigabytes, or terabytes to understand if you have the right tools and platform.
        1.6.4 Data Volume Limitations: Old source systems might struggle with performance when extracting large amounts of data, potentially impacting the source system's performance.
        1.6.5 Impact on Source System Performance : If given database access, be responsible and avoid impacting the database performance.
        1.6.6 Authentication and Authorization : Understand how to access data in the source system (tokens, SSH keys, passwords, IP Whitelistings etc.).
        
STEP 2 - Coding 
		2.1 Data Ingestion; How to load data from source to datawarehouse , creating a bridge between source system.
        2.1.1 Bronze rule table names - All names must start with the source system name and table names must match their original names without renaming. 
        <sourcesystem>_<entity> ; 
			<sourcesystem> : Name of the source system (e.g. crm, erp); 
            <entity> Exact table from the source system;
            Example: crm_customer_info - Customer information from CRM system.
        2.2 Creating DDL Scripts: Creating tables in the bronze layer, following naming conventions.
        2.3 Creating Stored Procedure : Creating a stored procedure to load the bronze layer.
STEP 3 - Validating 
	3.1 Data Validation ; Data completeness and Schema Checks , compare number of records between source system and data layer
STEP 4 - Docs and Version
	4.1 Data Documenting and Versioning in GIT

===============================================================================
Data Definition Language (DDL) Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

-- Create Database/Schemas

DROP DATABASE IF EXISTS DataWarehouse;
CREATE DATABASE IF NOT EXISTS DataWarehouse;
USE DataWarehouse;
DROP DATABASE DataWarehouse;

DROP DATABASE IF EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS bronze;
DROP DATABASE IF EXISTS silver;
CREATE DATABASE IF NOT EXISTS silver;
DROP DATABASE IF EXISTS gold;
CREATE SCHEMA IF NOT EXISTS gold;
USE bronze;

DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
	cst_id INT ,
    cst_key VARCHAR(50),
    cst_firstname  VARCHAR(50),
    cst_lastname  VARCHAR(50),
    cst_marital_status  VARCHAR(5),
    cst_gndr  VARCHAR(5),
    cst_create_date  DATE
);


DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
	prd_id INT ,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(5),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);

DROP TABLE bronze.crm_prd_info;

DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num VARCHAR(50) ,
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
	cid VARCHAR(50) PRIMARY KEY,
    badte DATE,
    gen VARCHAR(10)
);

DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
	cid VARCHAR(50),
    cnty VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
	id VARCHAR(20) PRIMARY KEY,
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50)
);

SELECT '===================' AS MESSAGE;
SELECT 'Loading CRM Tables' AS MESSAGE;
SELECT '===================' AS MESSAGE;
-- Capture Start Time
SET @start_time = NOW(); 
-- simulate 3 seconds pause
DO SLEEP (3);
SET GLOBAL local_infile = 1;
-- Step 1: Empty the table for fresh write up
TRUNCATE TABLE bronze.crm_cust_info;
-- Step 2: Lock the table for write
LOCK TABLE bronze.crm_cust_info WRITE;
-- Step 3: Load the Data
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cust_info.csv'
IGNORE
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;
SHOW WARNINGS LIMIT 20;
-- Step 4: Unlock the table for write
UNLOCK TABLES;
-- Step1: Empty the table for fresh data load
TRUNCATE TABLE bronze.crm_sales_details;
-- Step 2: Lock the table for write up
LOCK TABLE bronze.crm_sales_details WRITE;
-- Step 3 Load the data
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/sales_details.csv'
IGNORE
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;
SHOW WARNINGS LIMIT 20;
-- Step 4 Unlock the table for write
UNLOCK TABLES;
-- Step 1 Empty the table for fresh data load
TRUNCATE TABLE bronze.crm_prd_info;
-- Step 2 Lock the table for data load
LOCK TABLE bronze.crm_prd_info WRITE;
-- Step 3 Start data load 
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/prd_info.csv'
IGNORE
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;
SHOW WARNINGS LIMIT 20;
-- Step 4 Unlock the table
UNLOCK TABLES;

SELECT '==================' AS MESSAGE;
SELECT 'Loading ERP Tables' AS MESSAGE;
SELECT '==================' AS MESSAGE;
-- Step 1 Empty the table for fresh data load
TRUNCATE TABLE bronze.erp_cust_az12;
-- Step 2 Lock the table for data load
LOCK TABLE bronze.erp_cust_az12 WRITE;
-- Step 3 Start data load
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cust_az12.csv'
IGNORE
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;
SHOW WARNINGS LIMIT 20;
-- Step 4 Unlock the table
UNLOCK TABLE;
-- Step 1 Empty the table for data load
TRUNCATE TABLE bronze.erp_loc_a101;
-- Step 2 Lock the table for data load
LOCK TABLE bronze.erp_loc_a101 WRITE;
-- Step 3 Start data Load
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/loc_a101.csv'
IGNORE
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;
SHOW WARNINGS LIMIT 20;
-- Step 4 Unlock the table
UNLOCK TABLE;
-- Step 1 Empty Table for Data Load
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
-- Step 2 Lock Table for Data load
LOCK TABLE bronze.erp_px_cat_g1v2 WRITE;
-- Step 3 Load date in Table
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/px_cat_g1v2.csv'
IGNORE
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;
SHOW WARNINGS LIMIT 20;
-- Step 4 Unlock the table
UNLOCK TABLE;

SELECT'======================' AS MESSAGE;
SELECT 'Loading Data in Bronze Layer Complete' AS MESSAGE;
-- Capture end time
SET @end_time = NOW();
-- Calculate the difference in Seconds
SELECT TIMESTAMPDIFF(SECOND, @start_time, @end_time) AS duration_seconds;

--  -------------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------------
