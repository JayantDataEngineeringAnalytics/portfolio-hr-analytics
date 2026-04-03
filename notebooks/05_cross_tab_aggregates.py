# Databricks notebook source
# MAGIC %md
# MAGIC # Portfolio HR Analytics — Cross-Tab Aggregates for HTML Report
# MAGIC
# MAGIC **Source:** `portfolio_hr.gold.flight_risk_scores` + `portfolio_hr.silver.employees_clean`
# MAGIC **Purpose:** Pre-compute all values embedded in `hr-analytics.html` static report
# MAGIC
# MAGIC The HTML report uses **zero runtime database calls** — all numbers are embedded as
# MAGIC JavaScript constants. This notebook documents every value and its source query.
# MAGIC
# MAGIC ### HTML Report Sections
# MAGIC | Section | Data Source |
# MAGIC |---------|-------------|
# MAGIC | KPI Cards (4) | `agg_kpi_summary` |
# MAGIC | Attrition by Tenure (bar chart) | `agg_attrition_by_tenure` |
# MAGIC | Top Roles by Attrition (table) | `agg_attrition_by_role` |
# MAGIC | Flight Risk by Department (donut-table) | `agg_flight_risk_by_dept` |
# MAGIC | Risk Score Distribution (bar) | `flight_risk_scores` aggregated |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI Summary Values

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   total_employees,
# MAGIC   attrition_rate,
# MAGIC   avg_monthly_income,
# MAGIC   avg_tenure_years
# MAGIC FROM portfolio_hr.gold.agg_kpi_summary
# MAGIC -- Used in HTML: KPI cards
# MAGIC -- total_employees → "Total Employees"
# MAGIC -- attrition_rate  → "Attrition Rate"
# MAGIC -- avg_monthly_income → "Avg Monthly Income"
# MAGIC -- avg_tenure_years → "Avg Tenure"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Attrition by Tenure Band (Bar Chart)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   tenure_band,
# MAGIC   sort_order,
# MAGIC   headcount,
# MAGIC   attrited,
# MAGIC   attrition_rate
# MAGIC FROM portfolio_hr.gold.agg_attrition_by_tenure
# MAGIC ORDER BY sort_order
# MAGIC -- Used in HTML: buildTenureChart()
# MAGIC -- tenure_band → x-axis labels
# MAGIC -- attrition_rate → bar heights

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top Job Roles by Attrition (Table)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   job_role,
# MAGIC   department,
# MAGIC   headcount,
# MAGIC   attrited,
# MAGIC   attrition_rate,
# MAGIC   avg_monthly_income,
# MAGIC   avg_tenure_years
# MAGIC FROM portfolio_hr.gold.agg_attrition_by_role
# MAGIC ORDER BY attrition_rate DESC
# MAGIC -- Used in HTML: buildRolesTable()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flight Risk by Department

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   department,
# MAGIC   risk_band,
# MAGIC   employees,
# MAGIC   pct_in_dept,
# MAGIC   avg_score,
# MAGIC   actual_attrited
# MAGIC FROM portfolio_hr.gold.agg_flight_risk_by_dept
# MAGIC ORDER BY department, CASE risk_band WHEN 'Low' THEN 1 WHEN 'Medium' THEN 2 WHEN 'High' THEN 3 WHEN 'Critical' THEN 4 END
# MAGIC -- Used in HTML: buildRiskByDeptTable()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flight Risk Score Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   risk_band,
# MAGIC   COUNT(*) AS employees,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct_total,
# MAGIC   ROUND(AVG(flight_risk_score), 1) AS avg_score,
# MAGIC   SUM(CASE WHEN attrition_flag THEN 1 ELSE 0 END) AS actual_attrited,
# MAGIC   ROUND(SUM(CASE WHEN attrition_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS attrition_rate
# MAGIC FROM portfolio_hr.gold.flight_risk_scores
# MAGIC GROUP BY risk_band
# MAGIC ORDER BY CASE risk_band WHEN 'Low' THEN 1 WHEN 'Medium' THEN 2 WHEN 'High' THEN 3 WHEN 'Critical' THEN 4 END
# MAGIC -- Used in HTML: buildRiskBandChart()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Full Embedded Data Reference
# MAGIC
# MAGIC After running all queries above, embed results in `hr-analytics.html` as:
# MAGIC
# MAGIC ```javascript
# MAGIC const HR_DATA = {
# MAGIC   kpi: {
# MAGIC     total_employees: 1470,
# MAGIC     attrition_rate: 16.1,
# MAGIC     avg_monthly_income: 6503,
# MAGIC     avg_tenure_years: 7.0
# MAGIC   },
# MAGIC   tenure_chart: [
# MAGIC     { band: '0-1 yr',  headcount: 199, attrited: 65, rate: 32.7 },
# MAGIC     { band: '1-3 yr',  headcount: 222, attrited: 63, rate: 28.4 },
# MAGIC     { band: '3-5 yr',  headcount: 196, attrited: 34, rate: 17.3 },
# MAGIC     { band: '5-10 yr', headcount: 398, attrited: 51, rate: 12.8 },
# MAGIC     { band: '10+ yr',  headcount: 455, attrited: 24, rate:  5.3 }
# MAGIC   ],
# MAGIC   roles_table: [
# MAGIC     { role: 'Sales Representative',    dept: 'Sales',          n: 83, attrited: 33, rate: 39.8, income: 2626,  tenure: 3.3 },
# MAGIC     { role: 'Human Resources',         dept: 'Human Resources',n: 52, attrited: 12, rate: 23.1, income: 4245,  tenure: 5.2 },
# MAGIC     { role: 'Laboratory Technician',   dept: 'Research & Development', n: 259, attrited: 62, rate: 23.9, income: 3237, tenure: 4.9 },
# MAGIC     { role: 'Sales Executive',         dept: 'Sales',          n: 326, attrited: 57, rate: 17.5, income: 6924, tenure: 7.1 },
# MAGIC     { role: 'Research Scientist',      dept: 'Research & Development', n: 292, attrited: 47, rate: 16.1, income: 3239, tenure: 6.5 },
# MAGIC     { role: 'Manufacturing Director',  dept: 'Research & Development', n: 145, attrited: 10, rate:  6.9, income: 7295, tenure: 9.6 },
# MAGIC     { role: 'Healthcare Representative',dept:'Research & Development',n:131, attrited: 9, rate:  6.9, income: 7529, tenure: 9.5 },
# MAGIC     { role: 'Manager',                 dept: 'various',        n: 102, attrited:  5, rate:  4.9, income:17182, tenure:12.5 },
# MAGIC     { role: 'Research Director',       dept: 'Research & Development', n: 80, attrited:  2, rate:  2.5, income:16033, tenure:14.4 }
# MAGIC   ],
# MAGIC   risk_bands: [
# MAGIC     { band: 'Low',      n: null, pct: null, attrition_rate: null },
# MAGIC     { band: 'Medium',   n: null, pct: null, attrition_rate: null },
# MAGIC     { band: 'High',     n: null, pct: null, attrition_rate: null },
# MAGIC     { band: 'Critical', n: null, pct: null, attrition_rate: null }
# MAGIC   ]
# MAGIC   // NOTE: Fill nulls above from actual Databricks query results
# MAGIC };
# MAGIC ```
# MAGIC
# MAGIC **Run all queries above, copy exact values into `hr-analytics.html` REPORT_DATA object.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Lineage Map
# MAGIC
# MAGIC ```
# MAGIC WA_Fn-UseC_-HR-Employee-Attrition.csv
# MAGIC   → landing_zone.raw_files (Volume)
# MAGIC     → bronze.employees_raw              [01_bronze_ingestion.py]
# MAGIC       → silver.employees_clean          [02_silver_transform.py]
# MAGIC         → gold.fact_employees           [03_gold_aggregates.py]
# MAGIC         → gold.agg_attrition_by_dept    [03_gold_aggregates.py]
# MAGIC         → gold.agg_attrition_by_tenure  [03_gold_aggregates.py]  → HTML bar chart
# MAGIC         → gold.agg_attrition_by_role    [03_gold_aggregates.py]  → HTML table
# MAGIC         → gold.agg_salary_benchmarks    [03_gold_aggregates.py]
# MAGIC         → gold.agg_satisfaction_vs_attrition [03_gold_aggregates.py]
# MAGIC         → gold.agg_overtime_impact      [03_gold_aggregates.py]
# MAGIC         → gold.agg_manager_effectiveness [03_gold_aggregates.py]
# MAGIC         → gold.agg_kpi_summary          [03_gold_aggregates.py]  → HTML KPI cards
# MAGIC         → gold.flight_risk_scores       [04_flight_risk_model.py] → HTML risk chart
# MAGIC         → gold.agg_flight_risk_by_dept  [04_flight_risk_model.py] → HTML risk table
# MAGIC ```
