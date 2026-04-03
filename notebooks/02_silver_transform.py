# Databricks notebook source
# MAGIC %md
# MAGIC # Portfolio HR Analytics — Silver Layer Transformation
# MAGIC
# MAGIC **Source:** `portfolio_hr.bronze.employees_raw`
# MAGIC **Target:** `portfolio_hr.silver.employees_clean`
# MAGIC
# MAGIC Applies business logic, derived columns, and label mappings.
# MAGIC All columns renamed to snake_case. Data quality validated.
# MAGIC
# MAGIC ### Derived Columns
# MAGIC | Column | Logic |
# MAGIC |--------|-------|
# MAGIC | `tenure_band` | 0–1yr, 1–3yr, 3–5yr, 5–10yr, 10yr+ |
# MAGIC | `age_band` | 20s, 30s, 40s, 50s+ |
# MAGIC | `salary_band` | Percentile within job role: Low / Mid / High / Top |
# MAGIC | `income_percentile` | PERCENT_RANK() within full dataset |
# MAGIC | `attrition_flag` | Boolean: Yes → true, No → false |
# MAGIC | `environment_satisfaction_label` | 1=Low, 2=Medium, 3=High, 4=Very High |
# MAGIC | `job_satisfaction_label` | same mapping |
# MAGIC | `relationship_satisfaction_label` | same mapping |
# MAGIC | `work_life_balance_label` | 1=Bad, 2=Good, 3=Better, 4=Best |
# MAGIC | `job_involvement_label` | 1=Low, 2=Medium, 3=High, 4=Very High |
# MAGIC | `education_label` | 1=Below College, 2=College, 3=Bachelor, 4=Master, 5=Doctor |
# MAGIC | `performance_label` | 3=Excellent, 4=Outstanding |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Silver Schema (if not exists)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS portfolio_hr.silver
# MAGIC COMMENT 'Silver layer: cleaned and enriched HR data with derived business columns'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build employees_clean Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE portfolio_hr.silver.employees_clean
# MAGIC COMMENT 'Cleaned and enriched IBM HR dataset. Derived bands, labels, flags, and percentiles applied.'
# MAGIC AS
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     EmployeeNumber                          AS employee_id,
# MAGIC     Age                                     AS age,
# MAGIC     Gender                                  AS gender,
# MAGIC     MaritalStatus                           AS marital_status,
# MAGIC     Education                               AS education_level,
# MAGIC     EducationField                          AS education_field,
# MAGIC     Department                              AS department,
# MAGIC     JobRole                                 AS job_role,
# MAGIC     JobLevel                                AS job_level,
# MAGIC     BusinessTravel                          AS business_travel,
# MAGIC     OverTime                                AS overtime,
# MAGIC     DistanceFromHome                        AS distance_from_home,
# MAGIC     Attrition                               AS attrition,
# MAGIC     CASE WHEN Attrition = 'Yes' THEN true ELSE false END AS attrition_flag,
# MAGIC     MonthlyIncome                           AS monthly_income,
# MAGIC     DailyRate                               AS daily_rate,
# MAGIC     HourlyRate                              AS hourly_rate,
# MAGIC     MonthlyRate                             AS monthly_rate,
# MAGIC     PercentSalaryHike                       AS percent_salary_hike,
# MAGIC     StockOptionLevel                        AS stock_option_level,
# MAGIC     YearsAtCompany                          AS years_at_company,
# MAGIC     YearsInCurrentRole                      AS years_in_current_role,
# MAGIC     YearsSinceLastPromotion                 AS years_since_last_promotion,
# MAGIC     YearsWithCurrManager                    AS years_with_curr_manager,
# MAGIC     TotalWorkingYears                       AS total_working_years,
# MAGIC     NumCompaniesWorked                      AS num_companies_worked,
# MAGIC     TrainingTimesLastYear                   AS training_times_last_year,
# MAGIC     EnvironmentSatisfaction                 AS environment_satisfaction,
# MAGIC     JobSatisfaction                         AS job_satisfaction,
# MAGIC     RelationshipSatisfaction                AS relationship_satisfaction,
# MAGIC     WorkLifeBalance                         AS work_life_balance,
# MAGIC     JobInvolvement                          AS job_involvement,
# MAGIC     PerformanceRating                       AS performance_rating,
# MAGIC     -- Derived: age band
# MAGIC     CASE
# MAGIC       WHEN Age < 30 THEN 'Under 30'
# MAGIC       WHEN Age < 40 THEN '30s'
# MAGIC       WHEN Age < 50 THEN '40s'
# MAGIC       ELSE '50s+'
# MAGIC     END                                     AS age_band,
# MAGIC     -- Derived: tenure band
# MAGIC     CASE
# MAGIC       WHEN YearsAtCompany <= 1  THEN '0-1 yr'
# MAGIC       WHEN YearsAtCompany <= 3  THEN '1-3 yr'
# MAGIC       WHEN YearsAtCompany <= 5  THEN '3-5 yr'
# MAGIC       WHEN YearsAtCompany <= 10 THEN '5-10 yr'
# MAGIC       ELSE '10+ yr'
# MAGIC     END                                     AS tenure_band,
# MAGIC     -- Derived: satisfaction labels
# MAGIC     CASE EnvironmentSatisfaction
# MAGIC       WHEN 1 THEN 'Low' WHEN 2 THEN 'Medium'
# MAGIC       WHEN 3 THEN 'High' WHEN 4 THEN 'Very High'
# MAGIC     END                                     AS environment_satisfaction_label,
# MAGIC     CASE JobSatisfaction
# MAGIC       WHEN 1 THEN 'Low' WHEN 2 THEN 'Medium'
# MAGIC       WHEN 3 THEN 'High' WHEN 4 THEN 'Very High'
# MAGIC     END                                     AS job_satisfaction_label,
# MAGIC     CASE RelationshipSatisfaction
# MAGIC       WHEN 1 THEN 'Low' WHEN 2 THEN 'Medium'
# MAGIC       WHEN 3 THEN 'High' WHEN 4 THEN 'Very High'
# MAGIC     END                                     AS relationship_satisfaction_label,
# MAGIC     CASE WorkLifeBalance
# MAGIC       WHEN 1 THEN 'Bad' WHEN 2 THEN 'Good'
# MAGIC       WHEN 3 THEN 'Better' WHEN 4 THEN 'Best'
# MAGIC     END                                     AS work_life_balance_label,
# MAGIC     CASE JobInvolvement
# MAGIC       WHEN 1 THEN 'Low' WHEN 2 THEN 'Medium'
# MAGIC       WHEN 3 THEN 'High' WHEN 4 THEN 'Very High'
# MAGIC     END                                     AS job_involvement_label,
# MAGIC     CASE Education
# MAGIC       WHEN 1 THEN 'Below College' WHEN 2 THEN 'College'
# MAGIC       WHEN 3 THEN 'Bachelor' WHEN 4 THEN 'Master' WHEN 5 THEN 'Doctor'
# MAGIC     END                                     AS education_label,
# MAGIC     CASE PerformanceRating
# MAGIC       WHEN 3 THEN 'Excellent' WHEN 4 THEN 'Outstanding'
# MAGIC       ELSE 'Other'
# MAGIC     END                                     AS performance_label,
# MAGIC     -- Salary percentile within full dataset
# MAGIC     ROUND(PERCENT_RANK() OVER (ORDER BY MonthlyIncome) * 100, 1) AS income_percentile,
# MAGIC     _ingested_at,
# MAGIC     _source_file
# MAGIC   FROM portfolio_hr.bronze.employees_raw
# MAGIC ),
# MAGIC with_salary_band AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     -- Salary band within job role
# MAGIC     CASE
# MAGIC       WHEN PERCENT_RANK() OVER (PARTITION BY job_role ORDER BY monthly_income) < 0.25 THEN 'Low'
# MAGIC       WHEN PERCENT_RANK() OVER (PARTITION BY job_role ORDER BY monthly_income) < 0.50 THEN 'Mid'
# MAGIC       WHEN PERCENT_RANK() OVER (PARTITION BY job_role ORDER BY monthly_income) < 0.75 THEN 'High'
# MAGIC       ELSE 'Top'
# MAGIC     END AS salary_band
# MAGIC   FROM base
# MAGIC )
# MAGIC SELECT * FROM with_salary_band

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Validate Row Count and New Columns

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total FROM portfolio_hr.silver.employees_clean
# MAGIC -- Expected: 1470

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   tenure_band,
# MAGIC   COUNT(*) AS employees,
# MAGIC   SUM(CASE WHEN attrition_flag THEN 1 ELSE 0 END) AS attrited,
# MAGIC   ROUND(SUM(CASE WHEN attrition_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS attrition_pct
# MAGIC FROM portfolio_hr.silver.employees_clean
# MAGIC GROUP BY tenure_band
# MAGIC ORDER BY
# MAGIC   CASE tenure_band
# MAGIC     WHEN '0-1 yr'  THEN 1
# MAGIC     WHEN '1-3 yr'  THEN 2
# MAGIC     WHEN '3-5 yr'  THEN 3
# MAGIC     WHEN '5-10 yr' THEN 4
# MAGIC     WHEN '10+ yr'  THEN 5
# MAGIC   END

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   salary_band,
# MAGIC   COUNT(*) AS employees,
# MAGIC   ROUND(AVG(monthly_income), 0) AS avg_income,
# MAGIC   SUM(CASE WHEN attrition_flag THEN 1 ELSE 0 END) AS attrited,
# MAGIC   ROUND(SUM(CASE WHEN attrition_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS attrition_pct
# MAGIC FROM portfolio_hr.silver.employees_clean
# MAGIC GROUP BY salary_band
# MAGIC ORDER BY CASE salary_band WHEN 'Low' THEN 1 WHEN 'Mid' THEN 2 WHEN 'High' THEN 3 WHEN 'Top' THEN 4 END

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   age_band,
# MAGIC   COUNT(*) AS employees,
# MAGIC   ROUND(AVG(monthly_income), 0) AS avg_income,
# MAGIC   SUM(CASE WHEN attrition_flag THEN 1 ELSE 0 END) AS attrited
# MAGIC FROM portfolio_hr.silver.employees_clean
# MAGIC GROUP BY age_band
# MAGIC ORDER BY CASE age_band WHEN 'Under 30' THEN 1 WHEN '30s' THEN 2 WHEN '40s' THEN 3 WHEN '50s+' THEN 4 END

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Describe Silver Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE portfolio_hr.silver.employees_clean

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Silver transformation adds **12 derived columns** on top of the 35 raw columns:
# MAGIC
# MAGIC | Column | Type | Purpose |
# MAGIC |--------|------|---------|
# MAGIC | `attrition_flag` | boolean | ML-ready target variable |
# MAGIC | `age_band` | string | Demographic segmentation |
# MAGIC | `tenure_band` | string | Retention cohort analysis |
# MAGIC | `salary_band` | string | Compensation equity analysis |
# MAGIC | `income_percentile` | double | Cross-employee benchmarking |
# MAGIC | `environment_satisfaction_label` | string | Human-readable satisfaction |
# MAGIC | `job_satisfaction_label` | string | Human-readable satisfaction |
# MAGIC | `relationship_satisfaction_label` | string | Human-readable satisfaction |
# MAGIC | `work_life_balance_label` | string | Human-readable WLB |
# MAGIC | `job_involvement_label` | string | Human-readable involvement |
# MAGIC | `education_label` | string | Human-readable education |
# MAGIC | `performance_label` | string | Human-readable performance |
# MAGIC
# MAGIC **Next step:** `03_gold_aggregates.py` — build fact and aggregate tables for reporting
