# Data Dictionary — Gold Layer Tables

All tables reside in the `portfolio_hr.gold` schema.

---

## fact_employees

One row per employee — the central fact table for all HR BI reporting. Sourced from `silver.employees_clean`.

| Column | Type | Description |
|---|---|---|
| employee_id | INT | Unique employee identifier (EmployeeNumber) |
| department | STRING | HR / Research & Development / Sales |
| job_role | STRING | One of 9 job roles |
| job_level | INT | 1 (junior) → 5 (executive) |
| age | INT | Employee age in years |
| gender | STRING | Male / Female |
| marital_status | STRING | Single / Married / Divorced |
| education_level | INT | 1–5 (mapped in education_label) |
| education_field | STRING | Field of study |
| business_travel | STRING | Non-Travel / Travel_Rarely / Travel_Frequently |
| overtime | STRING | Yes / No |
| distance_from_home | INT | Distance from workplace in km |
| attrition | STRING | Yes / No (original string) |
| attrition_flag | BOOLEAN | True = employee left |
| monthly_income | INT | Gross monthly income (USD) |
| daily_rate | INT | Daily rate for compensation calc |
| hourly_rate | INT | Hourly rate |
| monthly_rate | INT | Monthly rate (separate from income) |
| percent_salary_hike | INT | Last salary hike % |
| stock_option_level | INT | 0–3 |
| years_at_company | INT | Total tenure at company |
| years_in_current_role | INT | Tenure in current role |
| years_since_last_promotion | INT | Years since last promotion |
| years_with_curr_manager | INT | Years reporting to current manager |
| total_working_years | INT | Total career experience |
| num_companies_worked | INT | Number of previous employers |
| training_times_last_year | INT | Training sessions attended |
| environment_satisfaction | INT | 1=Low → 4=Very High |
| job_satisfaction | INT | 1=Low → 4=Very High |
| relationship_satisfaction | INT | 1=Low → 4=Very High |
| work_life_balance | INT | 1=Bad → 4=Best |
| job_involvement | INT | 1=Low → 4=Very High |
| performance_rating | INT | 3=Excellent / 4=Outstanding |
| age_band | STRING | Under 30 / 30s / 40s / 50s+ |
| tenure_band | STRING | 0-1 yr / 1-3 yr / 3-5 yr / 5-10 yr / 10+ yr |
| salary_band | STRING | Low / Mid / High / Top (within job role) |
| income_percentile | DOUBLE | Income rank across all employees (0–100) |
| environment_satisfaction_label | STRING | Low / Medium / High / Very High |
| job_satisfaction_label | STRING | Low / Medium / High / Very High |
| relationship_satisfaction_label | STRING | Low / Medium / High / Very High |
| work_life_balance_label | STRING | Bad / Good / Better / Best |
| job_involvement_label | STRING | Low / Medium / High / Very High |
| education_label | STRING | Below College / College / Bachelor / Master / Doctor |
| performance_label | STRING | Excellent / Outstanding |

---

## flight_risk_scores

Per-employee flight risk score (0–100) derived from 10 weighted HR risk factors.

| Column | Type | Description |
|---|---|---|
| employee_id | INT | FK → fact_employees |
| department | STRING | Department |
| job_role | STRING | Job role |
| flight_risk_score | DOUBLE | Normalized score 0–100 (higher = more at risk) |
| raw_score | INT | Raw sum of factor points (max 116) |
| risk_band | STRING | Low / Medium / High / Critical |
| attrition_flag | BOOLEAN | Actual outcome — used for model validation |
| score_overtime | INT | 25 pts if overtime = Yes |
| score_salary | INT | 20 pts if salary_band = Low |
| score_new_hire | INT | 15 pts if years_at_company ≤ 1 |
| score_job_sat | INT | 15 pts if job_satisfaction ≤ 2 |
| score_env_sat | INT | 10 pts if environment_satisfaction ≤ 2 |
| score_wlb | INT | 10 pts if work_life_balance ≤ 2 |
| score_no_promo | INT | 8 pts if years_since_last_promotion ≥ 4 |
| score_distance | INT | 5 pts if distance_from_home ≥ 20 |
| score_new_mgr | INT | 5 pts if years_with_curr_manager ≤ 1 |
| score_job_hopper | INT | 3 pts if num_companies_worked ≥ 4 |

**Risk band validation (actual attrition rates):**
| Band | Score Range | Actual Attrition |
|------|-------------|-----------------|
| Low | 0–24 | 8.2% |
| Medium | 25–49 | 17.8% |
| High | 50–74 | 45.1% |
| Critical | 75–100 | 60.0% |

---

## agg_attrition_by_tenure

Attrition rate by tenure band — primary visual in HTML report.

| Column | Type | Description |
|---|---|---|
| tenure_band | STRING | 0-1 yr / 1-3 yr / 3-5 yr / 5-10 yr / 10+ yr |
| sort_order | INT | 1–5 for ordered display |
| headcount | LONG | Total employees in band |
| attrited | LONG | Employees who left |
| attrition_rate | DOUBLE | % attrition within band |
| avg_monthly_income | DOUBLE | Avg income in band |
| avg_age | DOUBLE | Avg age in band |

---

## agg_attrition_by_role

Attrition rate per job role — used in HTML report table.

| Column | Type | Description |
|---|---|---|
| job_role | STRING | Job role name |
| department | STRING | Parent department |
| headcount | LONG | Total employees |
| attrited | LONG | Employees who left |
| attrition_rate | DOUBLE | % attrition |
| avg_monthly_income | DOUBLE | Avg monthly income |
| avg_tenure_years | DOUBLE | Avg years at company |
| avg_job_satisfaction | DOUBLE | Avg satisfaction score (1–4) |

---

## agg_attrition_by_dept

Attrition summary per department.

| Column | Type | Description |
|---|---|---|
| department | STRING | HR / Research & Development / Sales |
| headcount | LONG | Total employees |
| attrited | LONG | Employees who left |
| attrition_rate | DOUBLE | % attrition |
| avg_monthly_income | DOUBLE | Avg income |
| avg_tenure_years | DOUBLE | Avg tenure |
| avg_job_satisfaction | DOUBLE | Avg satisfaction |
| avg_wlb_score | DOUBLE | Avg work-life balance score |

---

## agg_kpi_summary

Single-row KPI summary for HTML report header cards.

| Column | Type | Description |
|---|---|---|
| total_employees | LONG | 1,470 |
| total_attrited | LONG | 237 |
| attrition_rate | DOUBLE | 16.1% |
| avg_monthly_income | DOUBLE | $6,503 |
| avg_tenure_years | DOUBLE | 7.0 years |
| avg_job_satisfaction | DOUBLE | 2.73 |
| avg_wlb | DOUBLE | 2.76 |
| departments | LONG | 3 |
| job_roles | LONG | 9 |
| overtime_count | LONG | 416 |
| overtime_rate | DOUBLE | 28.3% |

---

## agg_salary_benchmarks

Income statistics per job role for compensation benchmarking.

| Column | Type | Description |
|---|---|---|
| job_role | STRING | Job role |
| department | STRING | Department |
| headcount | LONG | Employees |
| min_income | INT | Minimum monthly income |
| avg_income | DOUBLE | Average monthly income |
| median_income | DOUBLE | Median monthly income |
| max_income | INT | Maximum monthly income |

---

## agg_satisfaction_vs_attrition

Avg satisfaction scores for attrited vs retained employees.

| Column | Type | Description |
|---|---|---|
| attrition_status | STRING | Yes / No |
| headcount | LONG | Employees |
| avg_job_satisfaction | DOUBLE | Avg job satisfaction (1–4) |
| avg_env_satisfaction | DOUBLE | Avg environment satisfaction |
| avg_rel_satisfaction | DOUBLE | Avg relationship satisfaction |
| avg_wlb | DOUBLE | Avg work-life balance |
| avg_job_involvement | DOUBLE | Avg job involvement |
| avg_income | DOUBLE | Avg monthly income |
| avg_tenure | DOUBLE | Avg years at company |

---

## agg_overtime_impact

Attrition correlation with overtime across departments.

| Column | Type | Description |
|---|---|---|
| overtime | STRING | Yes / No |
| department | STRING | Department |
| headcount | LONG | Employees |
| attrited | LONG | Employees who left |
| attrition_rate | DOUBLE | % attrition |

---

## agg_manager_effectiveness

Attrition rate bucketed by manager tenure.

| Column | Type | Description |
|---|---|---|
| manager_tenure_band | STRING | New Manager / 1-2 yr / 3-5 yr / 6+ yr |
| headcount | LONG | Employees |
| attrited | LONG | Employees who left |
| attrition_rate | DOUBLE | % attrition |
| avg_job_satisfaction | DOUBLE | Avg satisfaction under that manager tenure |

---

## agg_flight_risk_by_dept

Flight risk band distribution per department — used in HTML report.

| Column | Type | Description |
|---|---|---|
| department | STRING | Department |
| risk_band | STRING | Low / Medium / High / Critical |
| employees | LONG | Employees in that band |
| pct_in_dept | DOUBLE | % of department in that band |
| avg_score | DOUBLE | Avg flight risk score |
| actual_attrited | LONG | Employees who actually left |
