# Databricks notebook source
# MAGIC %md
# MAGIC # Portfolio HR Analytics — Gold Layer Aggregates
# MAGIC
# MAGIC **Source:** `portfolio_hr.silver.employees_clean`
# MAGIC **Schema:** `portfolio_hr.gold`
# MAGIC
# MAGIC Produces reporting-ready aggregate tables consumed by the Power BI dashboard and HTML report.
# MAGIC
# MAGIC ### Tables Created
# MAGIC | Table | Description |
# MAGIC |-------|-------------|
# MAGIC | `fact_employees` | One row per employee — all silver columns + gold enrichments |
# MAGIC | `agg_attrition_by_dept` | Attrition rate, avg income, avg tenure per department |
# MAGIC | `agg_attrition_by_tenure` | Attrition by tenure band |
# MAGIC | `agg_attrition_by_role` | Attrition by job role |
# MAGIC | `agg_salary_benchmarks` | Income stats per role × salary band |
# MAGIC | `agg_satisfaction_vs_attrition` | Avg satisfaction scores for attrited vs retained |
# MAGIC | `agg_overtime_impact` | Attrition split by overtime flag |
# MAGIC | `agg_manager_effectiveness` | YearsWithCurrManager vs attrition rate |
# MAGIC | `agg_kpi_summary` | Single-row KPI summary for dashboard header |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Gold Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS portfolio_hr.gold
# MAGIC COMMENT 'Gold layer: reporting-ready aggregates and fact tables for Power BI and HTML report'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Fact Employees

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE portfolio_hr.gold.fact_employees
# MAGIC COMMENT 'One row per employee with all silver columns. Grain: employee_id.'
# MAGIC AS
# MAGIC SELECT * FROM portfolio_hr.silver.employees_clean

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Attrition by Department

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE portfolio_hr.gold.agg_attrition_by_dept
# MAGIC COMMENT 'Attrition rate, headcount, income, and tenure per department.'
# MAGIC AS
# MAGIC SELECT
# MAGIC   department,
# MAGIC   COUNT(*)                                                          AS headcount,
# MAGIC   SUM(CASE WHEN attrition_flag THEN 1 ELSE 0 END)                  AS attrited,
# MAGIC   ROUND(SUM(CASE WHEN attrition_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS attrition_rate,
# MAGIC   ROUND(AVG(monthly_income), 0)                                     AS avg_monthly_income,
# MAGIC   ROUND(AVG(years_at_company), 1)                                   AS avg_tenure_years,
# MAGIC   ROUND(AVG(job_satisfaction), 2)                                   AS avg_job_satisfaction,
# MAGIC   ROUND(AVG(work_life_balance), 2)                                  AS avg_wlb_score
# MAGIC FROM portfolio_hr.silver.employees_clean
# MAGIC GROUP BY department
# MAGIC ORDER BY attrition_rate DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Attrition by Tenure Band

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE portfolio_hr.gold.agg_attrition_by_tenure
# MAGIC COMMENT 'Attrition rate by tenure band. Used for main bar chart in HTML report.'
# MAGIC AS
# MAGIC SELECT
# MAGIC   tenure_band,
# MAGIC   CASE tenure_band
# MAGIC     WHEN '0-1 yr'  THEN 1
# MAGIC     WHEN '1-3 yr'  THEN 2
# MAGIC     WHEN '3-5 yr'  THEN 3
# MAGIC     WHEN '5-10 yr' THEN 4
# MAGIC     WHEN '10+ yr'  THEN 5
# MAGIC   END                                                               AS sort_order,
# MAGIC   COUNT(*)                                                          AS headcount,
# MAGIC   SUM(CASE WHEN attrition_flag THEN 1 ELSE 0 END)                  AS attrited,
# MAGIC   ROUND(SUM(CASE WHEN attrition_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS attrition_rate,
# MAGIC   ROUND(AVG(monthly_income), 0)                                     AS avg_monthly_income,
# MAGIC   ROUND(AVG(age), 1)                                                AS avg_age
# MAGIC FROM portfolio_hr.silver.employees_clean
# MAGIC GROUP BY tenure_band
# MAGIC ORDER BY sort_order

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Attrition by Job Role

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE portfolio_hr.gold.agg_attrition_by_role
# MAGIC COMMENT 'Attrition rate, headcount, and avg income per job role. Used for top-roles table in HTML report.'
# MAGIC AS
# MAGIC SELECT
# MAGIC   job_role,
# MAGIC   department,
# MAGIC   COUNT(*)                                                          AS headcount,
# MAGIC   SUM(CASE WHEN attrition_flag THEN 1 ELSE 0 END)                  AS attrited,
# MAGIC   ROUND(SUM(CASE WHEN attrition_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS attrition_rate,
# MAGIC   ROUND(AVG(monthly_income), 0)                                     AS avg_monthly_income,
# MAGIC   ROUND(AVG(years_at_company), 1)                                   AS avg_tenure_years,
# MAGIC   ROUND(AVG(job_satisfaction), 2)                                   AS avg_job_satisfaction
# MAGIC FROM portfolio_hr.silver.employees_clean
# MAGIC GROUP BY job_role, department
# MAGIC ORDER BY attrition_rate DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Salary Benchmarks by Role

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE portfolio_hr.gold.agg_salary_benchmarks
# MAGIC COMMENT 'Income statistics per job role for compensation benchmarking.'
# MAGIC AS
# MAGIC SELECT
# MAGIC   job_role,
# MAGIC   department,
# MAGIC   COUNT(*)                        AS headcount,
# MAGIC   MIN(monthly_income)             AS min_income,
# MAGIC   ROUND(AVG(monthly_income), 0)   AS avg_income,
# MAGIC   ROUND(PERCENTILE(monthly_income, 0.5), 0) AS median_income,
# MAGIC   MAX(monthly_income)             AS max_income,
# MAGIC   ROUND(STDDEV(monthly_income), 0) AS stddev_income
# MAGIC FROM portfolio_hr.silver.employees_clean
# MAGIC GROUP BY job_role, department
# MAGIC ORDER BY avg_income DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Satisfaction vs Attrition

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE portfolio_hr.gold.agg_satisfaction_vs_attrition
# MAGIC COMMENT 'Average satisfaction scores compared between attrited and retained employees.'
# MAGIC AS
# MAGIC SELECT
# MAGIC   attrition                                       AS attrition_status,
# MAGIC   COUNT(*)                                        AS headcount,
# MAGIC   ROUND(AVG(job_satisfaction), 2)                 AS avg_job_satisfaction,
# MAGIC   ROUND(AVG(environment_satisfaction), 2)         AS avg_env_satisfaction,
# MAGIC   ROUND(AVG(relationship_satisfaction), 2)        AS avg_rel_satisfaction,
# MAGIC   ROUND(AVG(work_life_balance), 2)                AS avg_wlb,
# MAGIC   ROUND(AVG(job_involvement), 2)                  AS avg_job_involvement,
# MAGIC   ROUND(AVG(monthly_income), 0)                   AS avg_income,
# MAGIC   ROUND(AVG(years_at_company), 1)                 AS avg_tenure
# MAGIC FROM portfolio_hr.silver.employees_clean
# MAGIC GROUP BY attrition

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Overtime Impact

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE portfolio_hr.gold.agg_overtime_impact
# MAGIC COMMENT 'How overtime correlates with attrition rate across departments.'
# MAGIC AS
# MAGIC SELECT
# MAGIC   overtime,
# MAGIC   department,
# MAGIC   COUNT(*)                                                          AS headcount,
# MAGIC   SUM(CASE WHEN attrition_flag THEN 1 ELSE 0 END)                  AS attrited,
# MAGIC   ROUND(SUM(CASE WHEN attrition_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS attrition_rate
# MAGIC FROM portfolio_hr.silver.employees_clean
# MAGIC GROUP BY overtime, department
# MAGIC ORDER BY attrition_rate DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Manager Effectiveness

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE portfolio_hr.gold.agg_manager_effectiveness
# MAGIC COMMENT 'Attrition rate bucketed by years with current manager.'
# MAGIC AS
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN years_with_curr_manager = 0 THEN 'New Manager'
# MAGIC     WHEN years_with_curr_manager <= 2 THEN '1-2 yr'
# MAGIC     WHEN years_with_curr_manager <= 5 THEN '3-5 yr'
# MAGIC     ELSE '6+ yr'
# MAGIC   END                                                               AS manager_tenure_band,
# MAGIC   COUNT(*)                                                          AS headcount,
# MAGIC   SUM(CASE WHEN attrition_flag THEN 1 ELSE 0 END)                  AS attrited,
# MAGIC   ROUND(SUM(CASE WHEN attrition_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS attrition_rate,
# MAGIC   ROUND(AVG(job_satisfaction), 2)                                   AS avg_job_satisfaction
# MAGIC FROM portfolio_hr.silver.employees_clean
# MAGIC GROUP BY 1
# MAGIC ORDER BY attrition_rate DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: KPI Summary (Single Row)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE portfolio_hr.gold.agg_kpi_summary
# MAGIC COMMENT 'Single-row KPI summary for HTML report header cards.'
# MAGIC AS
# MAGIC SELECT
# MAGIC   COUNT(*)                                                          AS total_employees,
# MAGIC   SUM(CASE WHEN attrition_flag THEN 1 ELSE 0 END)                  AS total_attrited,
# MAGIC   ROUND(SUM(CASE WHEN attrition_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS attrition_rate,
# MAGIC   ROUND(AVG(monthly_income), 0)                                     AS avg_monthly_income,
# MAGIC   ROUND(AVG(years_at_company), 1)                                   AS avg_tenure_years,
# MAGIC   ROUND(AVG(job_satisfaction), 2)                                   AS avg_job_satisfaction,
# MAGIC   ROUND(AVG(work_life_balance), 2)                                  AS avg_wlb,
# MAGIC   COUNT(DISTINCT department)                                        AS departments,
# MAGIC   COUNT(DISTINCT job_role)                                          AS job_roles,
# MAGIC   SUM(CASE WHEN overtime = 'Yes' THEN 1 ELSE 0 END)                AS overtime_count,
# MAGIC   ROUND(SUM(CASE WHEN overtime = 'Yes' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS overtime_rate
# MAGIC FROM portfolio_hr.silver.employees_clean

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Validate All Gold Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'fact_employees'              AS tbl, COUNT(*) AS rows FROM portfolio_hr.gold.fact_employees
# MAGIC UNION ALL
# MAGIC SELECT 'agg_attrition_by_dept',        COUNT(*) FROM portfolio_hr.gold.agg_attrition_by_dept
# MAGIC UNION ALL
# MAGIC SELECT 'agg_attrition_by_tenure',      COUNT(*) FROM portfolio_hr.gold.agg_attrition_by_tenure
# MAGIC UNION ALL
# MAGIC SELECT 'agg_attrition_by_role',        COUNT(*) FROM portfolio_hr.gold.agg_attrition_by_role
# MAGIC UNION ALL
# MAGIC SELECT 'agg_salary_benchmarks',        COUNT(*) FROM portfolio_hr.gold.agg_salary_benchmarks
# MAGIC UNION ALL
# MAGIC SELECT 'agg_satisfaction_vs_attrition',COUNT(*) FROM portfolio_hr.gold.agg_satisfaction_vs_attrition
# MAGIC UNION ALL
# MAGIC SELECT 'agg_overtime_impact',          COUNT(*) FROM portfolio_hr.gold.agg_overtime_impact
# MAGIC UNION ALL
# MAGIC SELECT 'agg_manager_effectiveness',    COUNT(*) FROM portfolio_hr.gold.agg_manager_effectiveness
# MAGIC UNION ALL
# MAGIC SELECT 'agg_kpi_summary',              COUNT(*) FROM portfolio_hr.gold.agg_kpi_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM portfolio_hr.gold.agg_kpi_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM portfolio_hr.gold.agg_attrition_by_tenure ORDER BY sort_order

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM portfolio_hr.gold.agg_attrition_by_role ORDER BY attrition_rate DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Table | Grain | Primary Use |
# MAGIC |-------|-------|-------------|
# MAGIC | `fact_employees` | employee_id | Full dataset for PBI drill-through |
# MAGIC | `agg_attrition_by_dept` | department | PBI department slicer |
# MAGIC | `agg_attrition_by_tenure` | tenure_band | **HTML report bar chart** |
# MAGIC | `agg_attrition_by_role` | job_role | **HTML report table** |
# MAGIC | `agg_salary_benchmarks` | job_role | PBI compensation page |
# MAGIC | `agg_satisfaction_vs_attrition` | attrition_status | PBI satisfaction comparison |
# MAGIC | `agg_overtime_impact` | overtime × dept | PBI overtime analysis |
# MAGIC | `agg_manager_effectiveness` | manager_tenure_band | PBI manager analysis |
# MAGIC | `agg_kpi_summary` | (single row) | **HTML report KPI cards** |
# MAGIC
# MAGIC **Next step:** `04_flight_risk_model.py` — weighted flight risk score per employee
