# Databricks notebook source
# MAGIC %md
# MAGIC # Portfolio HR Analytics — Flight Risk Scoring Model
# MAGIC
# MAGIC **Source:** `portfolio_hr.silver.employees_clean`
# MAGIC **Target:** `portfolio_hr.gold.flight_risk_scores`
# MAGIC
# MAGIC Assigns each employee a weighted Flight Risk Score (0–100) based on known attrition predictors.
# MAGIC Employees are bucketed into 4 risk bands for targeted HR intervention.
# MAGIC
# MAGIC ### Risk Factor Weights
# MAGIC
# MAGIC | Factor | Weight | Rationale |
# MAGIC |--------|--------|-----------|
# MAGIC | Overtime = Yes | 25 pts | Strongest single predictor in literature |
# MAGIC | Salary band = Low | 20 pts | Low pay × market → high pull factor |
# MAGIC | Years at company ≤ 1 | 15 pts | First-year attrition is highest risk window |
# MAGIC | Job satisfaction ≤ 2 | 15 pts | Direct disengagement signal |
# MAGIC | Environment satisfaction ≤ 2 | 10 pts | Workplace fit signal |
# MAGIC | Work-life balance ≤ 2 | 10 pts | Burnout indicator |
# MAGIC | Years since last promotion ≥ 4 | 8 pts | Stagnation signal |
# MAGIC | Distance from home ≥ 20km | 5 pts | Commute burden |
# MAGIC | Years with curr manager ≤ 1 | 5 pts | New manager = transition risk |
# MAGIC | Num companies worked ≥ 4 | 3 pts | Job-hopping tendency |
# MAGIC
# MAGIC **Max possible score: 116** → normalized to 0–100
# MAGIC
# MAGIC ### Risk Bands
# MAGIC | Band | Score | Action |
# MAGIC |------|-------|--------|
# MAGIC | Low | 0–24 | Monitor quarterly |
# MAGIC | Medium | 25–49 | Engagement check-in |
# MAGIC | High | 50–74 | Manager intervention |
# MAGIC | Critical | 75–100 | Immediate HR action |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Compute Raw Scores

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE portfolio_hr.gold.flight_risk_scores
# MAGIC COMMENT 'Per-employee flight risk score (0-100) derived from weighted HR factors.'
# MAGIC AS
# MAGIC WITH raw_scores AS (
# MAGIC   SELECT
# MAGIC     employee_id,
# MAGIC     department,
# MAGIC     job_role,
# MAGIC     age,
# MAGIC     gender,
# MAGIC     marital_status,
# MAGIC     tenure_band,
# MAGIC     salary_band,
# MAGIC     monthly_income,
# MAGIC     income_percentile,
# MAGIC     overtime,
# MAGIC     attrition_flag,
# MAGIC     attrition,
# MAGIC     job_satisfaction,
# MAGIC     environment_satisfaction,
# MAGIC     work_life_balance,
# MAGIC     years_at_company,
# MAGIC     years_since_last_promotion,
# MAGIC     years_with_curr_manager,
# MAGIC     distance_from_home,
# MAGIC     num_companies_worked,
# MAGIC     -- Individual factor scores
# MAGIC     CASE WHEN overtime = 'Yes'                    THEN 25 ELSE 0 END AS score_overtime,
# MAGIC     CASE WHEN salary_band = 'Low'                 THEN 20 ELSE 0 END AS score_salary,
# MAGIC     CASE WHEN years_at_company <= 1               THEN 15 ELSE 0 END AS score_new_hire,
# MAGIC     CASE WHEN job_satisfaction <= 2               THEN 15 ELSE 0 END AS score_job_sat,
# MAGIC     CASE WHEN environment_satisfaction <= 2       THEN 10 ELSE 0 END AS score_env_sat,
# MAGIC     CASE WHEN work_life_balance <= 2              THEN 10 ELSE 0 END AS score_wlb,
# MAGIC     CASE WHEN years_since_last_promotion >= 4     THEN  8 ELSE 0 END AS score_no_promo,
# MAGIC     CASE WHEN distance_from_home >= 20            THEN  5 ELSE 0 END AS score_distance,
# MAGIC     CASE WHEN years_with_curr_manager <= 1        THEN  5 ELSE 0 END AS score_new_mgr,
# MAGIC     CASE WHEN num_companies_worked >= 4           THEN  3 ELSE 0 END AS score_job_hopper
# MAGIC   FROM portfolio_hr.silver.employees_clean
# MAGIC ),
# MAGIC totals AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     (score_overtime + score_salary + score_new_hire + score_job_sat +
# MAGIC      score_env_sat + score_wlb + score_no_promo + score_distance +
# MAGIC      score_new_mgr + score_job_hopper)                           AS raw_score,
# MAGIC     -- Normalize to 0-100 (max raw = 116)
# MAGIC     ROUND(
# MAGIC       (score_overtime + score_salary + score_new_hire + score_job_sat +
# MAGIC        score_env_sat + score_wlb + score_no_promo + score_distance +
# MAGIC        score_new_mgr + score_job_hopper) * 100.0 / 116, 1
# MAGIC     )                                                             AS flight_risk_score
# MAGIC   FROM raw_scores
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   CASE
# MAGIC     WHEN flight_risk_score < 25 THEN 'Low'
# MAGIC     WHEN flight_risk_score < 50 THEN 'Medium'
# MAGIC     WHEN flight_risk_score < 75 THEN 'High'
# MAGIC     ELSE 'Critical'
# MAGIC   END AS risk_band
# MAGIC FROM totals

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Validate Score Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   risk_band,
# MAGIC   COUNT(*)                                                          AS employees,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)               AS pct_of_total,
# MAGIC   SUM(CASE WHEN attrition_flag THEN 1 ELSE 0 END)                  AS actual_attrited,
# MAGIC   ROUND(SUM(CASE WHEN attrition_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS attrition_rate,
# MAGIC   ROUND(AVG(flight_risk_score), 1)                                  AS avg_score,
# MAGIC   ROUND(AVG(monthly_income), 0)                                     AS avg_income
# MAGIC FROM portfolio_hr.gold.flight_risk_scores
# MAGIC GROUP BY risk_band
# MAGIC ORDER BY CASE risk_band WHEN 'Low' THEN 1 WHEN 'Medium' THEN 2 WHEN 'High' THEN 3 WHEN 'Critical' THEN 4 END

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Model validation: higher risk band should have higher actual attrition
# MAGIC -- A well-calibrated model shows monotonically increasing attrition_rate across bands
# MAGIC SELECT
# MAGIC   risk_band,
# MAGIC   COUNT(*) AS n,
# MAGIC   ROUND(SUM(CASE WHEN attrition_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS actual_attrition_pct
# MAGIC FROM portfolio_hr.gold.flight_risk_scores
# MAGIC GROUP BY risk_band
# MAGIC ORDER BY CASE risk_band WHEN 'Low' THEN 1 WHEN 'Medium' THEN 2 WHEN 'High' THEN 3 WHEN 'Critical' THEN 4 END

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Top 10 Highest-Risk Employees (anonymized for demo)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   employee_id,
# MAGIC   department,
# MAGIC   job_role,
# MAGIC   tenure_band,
# MAGIC   salary_band,
# MAGIC   overtime,
# MAGIC   flight_risk_score,
# MAGIC   risk_band,
# MAGIC   score_overtime,
# MAGIC   score_salary,
# MAGIC   score_new_hire,
# MAGIC   score_job_sat,
# MAGIC   score_wlb,
# MAGIC   attrition AS actual_attrition
# MAGIC FROM portfolio_hr.gold.flight_risk_scores
# MAGIC WHERE risk_band = 'Critical'
# MAGIC ORDER BY flight_risk_score DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Risk by Department

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE portfolio_hr.gold.agg_flight_risk_by_dept
# MAGIC COMMENT 'Flight risk band distribution per department. Used in HTML report donut chart.'
# MAGIC AS
# MAGIC SELECT
# MAGIC   department,
# MAGIC   risk_band,
# MAGIC   COUNT(*) AS employees,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY department), 1) AS pct_in_dept,
# MAGIC   ROUND(AVG(flight_risk_score), 1) AS avg_score,
# MAGIC   SUM(CASE WHEN attrition_flag THEN 1 ELSE 0 END) AS actual_attrited
# MAGIC FROM portfolio_hr.gold.flight_risk_scores
# MAGIC GROUP BY department, risk_band
# MAGIC ORDER BY department, CASE risk_band WHEN 'Low' THEN 1 WHEN 'Medium' THEN 2 WHEN 'High' THEN 3 WHEN 'Critical' THEN 4 END

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM portfolio_hr.gold.agg_flight_risk_by_dept ORDER BY department, pct_in_dept DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Risk by Tenure Band

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE portfolio_hr.gold.agg_flight_risk_by_tenure
# MAGIC COMMENT 'Flight risk distribution per tenure band.'
# MAGIC AS
# MAGIC SELECT
# MAGIC   tenure_band,
# MAGIC   CASE tenure_band WHEN '0-1 yr' THEN 1 WHEN '1-3 yr' THEN 2 WHEN '3-5 yr' THEN 3 WHEN '5-10 yr' THEN 4 WHEN '10+ yr' THEN 5 END AS sort_order,
# MAGIC   risk_band,
# MAGIC   COUNT(*) AS employees,
# MAGIC   ROUND(AVG(flight_risk_score), 1) AS avg_score,
# MAGIC   SUM(CASE WHEN attrition_flag THEN 1 ELSE 0 END) AS actual_attrited
# MAGIC FROM portfolio_hr.gold.flight_risk_scores
# MAGIC GROUP BY tenure_band, risk_band
# MAGIC ORDER BY sort_order, CASE risk_band WHEN 'Low' THEN 1 WHEN 'Medium' THEN 2 WHEN 'High' THEN 3 WHEN 'Critical' THEN 4 END

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### Key Results (expected from IBM dataset)
# MAGIC
# MAGIC - **Critical risk** employees have ~4–5× higher actual attrition than Low risk → model is well-calibrated
# MAGIC - **Sales Representatives** typically concentrate in High/Critical bands due to overtime + low salary band combination
# MAGIC - **R&D Scientists** typically Low/Medium — higher pay, growth opportunities
# MAGIC - Overtime is the dominant single factor (25 pts = 21.6% of max score)
# MAGIC
# MAGIC ### Power BI DAX Measures (for PBI implementation)
# MAGIC
# MAGIC ```dax
# MAGIC // Flight Risk Score (recreate in PBI for live data)
# MAGIC Flight Risk Score =
# MAGIC   IF([Overtime] = "Yes", 25, 0)
# MAGIC   + IF([SalaryBand] = "Low", 20, 0)
# MAGIC   + IF([YearsAtCompany] <= 1, 15, 0)
# MAGIC   + IF([JobSatisfaction] <= 2, 15, 0)
# MAGIC   + IF([EnvironmentSatisfaction] <= 2, 10, 0)
# MAGIC   + IF([WorkLifeBalance] <= 2, 10, 0)
# MAGIC   + IF([YearsSinceLastPromotion] >= 4, 8, 0)
# MAGIC   + IF([DistanceFromHome] >= 20, 5, 0)
# MAGIC   + IF([YearsWithCurrManager] <= 1, 5, 0)
# MAGIC   + IF([NumCompaniesWorked] >= 4, 3, 0)
# MAGIC
# MAGIC // Risk Band
# MAGIC Risk Band =
# MAGIC   SWITCH(TRUE(),
# MAGIC     [Flight Risk Score] < 29, "Low",
# MAGIC     [Flight Risk Score] < 58, "Medium",
# MAGIC     [Flight Risk Score] < 87, "High",
# MAGIC     "Critical"
# MAGIC   )
# MAGIC
# MAGIC // % High/Critical
# MAGIC Pct High or Critical Risk =
# MAGIC   DIVIDE(
# MAGIC     COUNTROWS(FILTER(fact_employees, [Risk Band] IN {"High", "Critical"})),
# MAGIC     COUNTROWS(fact_employees)
# MAGIC   )
# MAGIC ```
# MAGIC
# MAGIC **Next step:** `05_cross_tab_aggregates.py` — aggregate tables for HTML report static data
