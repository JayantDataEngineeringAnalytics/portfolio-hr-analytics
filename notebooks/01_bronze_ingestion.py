# Databricks notebook source
# MAGIC %md
# MAGIC # Portfolio HR Analytics — Bronze Layer Ingestion
# MAGIC
# MAGIC **Catalog:** `portfolio_hr`
# MAGIC **Schema:** `portfolio_hr.bronze`
# MAGIC **Target table:** `portfolio_hr.bronze.employees_raw`
# MAGIC
# MAGIC Raw ingestion of IBM HR Attrition dataset from the landing zone volume.
# MAGIC No transformations applied — data preserved exactly as uploaded.
# MAGIC
# MAGIC **Source file:** `WA_Fn-UseC_-HR-Employee-Attrition.csv` (1,470 employees, 35 columns)
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Verify Landing Zone Volume

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '/Volumes/portfolio_hr/landing_zone/raw_files/'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Peek at Raw CSV

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM read_files(
# MAGIC   '/Volumes/portfolio_hr/landing_zone/raw_files/WA_Fn-UseC_-HR-Employee-Attrition.csv',
# MAGIC   format => 'csv',
# MAGIC   header => 'true',
# MAGIC   inferSchema => 'true'
# MAGIC )
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Bronze Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE portfolio_hr.bronze.employees_raw
# MAGIC COMMENT 'Raw IBM HR Attrition data ingested from landing zone. No transformations applied.'
# MAGIC AS
# MAGIC SELECT
# MAGIC   Age,
# MAGIC   Attrition,
# MAGIC   BusinessTravel,
# MAGIC   DailyRate,
# MAGIC   Department,
# MAGIC   DistanceFromHome,
# MAGIC   Education,
# MAGIC   EducationField,
# MAGIC   EmployeeCount,
# MAGIC   EmployeeNumber,
# MAGIC   EnvironmentSatisfaction,
# MAGIC   Gender,
# MAGIC   HourlyRate,
# MAGIC   JobInvolvement,
# MAGIC   JobLevel,
# MAGIC   JobRole,
# MAGIC   JobSatisfaction,
# MAGIC   MaritalStatus,
# MAGIC   MonthlyIncome,
# MAGIC   MonthlyRate,
# MAGIC   NumCompaniesWorked,
# MAGIC   OverTime,
# MAGIC   PercentSalaryHike,
# MAGIC   PerformanceRating,
# MAGIC   RelationshipSatisfaction,
# MAGIC   StandardHours,
# MAGIC   StockOptionLevel,
# MAGIC   TotalWorkingYears,
# MAGIC   TrainingTimesLastYear,
# MAGIC   WorkLifeBalance,
# MAGIC   YearsAtCompany,
# MAGIC   YearsInCurrentRole,
# MAGIC   YearsSinceLastPromotion,
# MAGIC   YearsWithCurrManager,
# MAGIC   current_timestamp() AS _ingested_at,
# MAGIC   'WA_Fn-UseC_-HR-Employee-Attrition.csv' AS _source_file
# MAGIC FROM read_files(
# MAGIC   '/Volumes/portfolio_hr/landing_zone/raw_files/WA_Fn-UseC_-HR-Employee-Attrition.csv',
# MAGIC   format => 'csv',
# MAGIC   header => 'true',
# MAGIC   inferSchema => 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Validate — Row Count and Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_employees
# MAGIC FROM portfolio_hr.bronze.employees_raw
# MAGIC -- Expected: 1470

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE portfolio_hr.bronze.employees_raw

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Basic Data Quality Checks

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                         AS total_rows,
# MAGIC   COUNT(DISTINCT EmployeeNumber)                   AS unique_employees,
# MAGIC   SUM(CASE WHEN Attrition = 'Yes' THEN 1 ELSE 0 END) AS attrited,
# MAGIC   SUM(CASE WHEN Attrition = 'No'  THEN 1 ELSE 0 END) AS retained,
# MAGIC   ROUND(SUM(CASE WHEN Attrition = 'Yes' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS attrition_rate_pct,
# MAGIC   COUNT(DISTINCT Department)                       AS departments,
# MAGIC   COUNT(DISTINCT JobRole)                          AS job_roles,
# MAGIC   MIN(Age) AS min_age,
# MAGIC   MAX(Age) AS max_age,
# MAGIC   ROUND(AVG(MonthlyIncome), 0)                     AS avg_monthly_income
# MAGIC FROM portfolio_hr.bronze.employees_raw
# MAGIC -- Expected: 1470 rows, 237 attrited (~16.1%), 3 departments, 9 job roles

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for NULLs in key columns
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN Age IS NULL THEN 1 ELSE 0 END)              AS null_age,
# MAGIC   SUM(CASE WHEN Attrition IS NULL THEN 1 ELSE 0 END)        AS null_attrition,
# MAGIC   SUM(CASE WHEN Department IS NULL THEN 1 ELSE 0 END)        AS null_department,
# MAGIC   SUM(CASE WHEN JobRole IS NULL THEN 1 ELSE 0 END)           AS null_jobrole,
# MAGIC   SUM(CASE WHEN MonthlyIncome IS NULL THEN 1 ELSE 0 END)     AS null_income,
# MAGIC   SUM(CASE WHEN YearsAtCompany IS NULL THEN 1 ELSE 0 END)    AS null_tenure
# MAGIC FROM portfolio_hr.bronze.employees_raw
# MAGIC -- Expected: 0 NULLs (clean dataset)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Metric | Value |
# MAGIC |--------|-------|
# MAGIC | Total employees | 1,470 |
# MAGIC | Attrition rate | 16.1% (237 employees) |
# MAGIC | Departments | 3 (HR, R&D, Sales) |
# MAGIC | Job roles | 9 |
# MAGIC | Age range | 18–60 |
# MAGIC | NULLs found | 0 |
# MAGIC
# MAGIC **Next step:** `02_silver_transform.py` — derive tenure bands, age bands, salary percentiles, satisfaction labels
