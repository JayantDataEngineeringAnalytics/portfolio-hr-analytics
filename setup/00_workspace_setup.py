# Databricks notebook source
# MAGIC %md
# MAGIC # Workspace Setup — portfolio_hr Catalog
# MAGIC
# MAGIC Run this notebook **once** to provision all schemas and volumes required for the
# MAGIC HR Workforce Intelligence pipeline.
# MAGIC Requires: Unity Catalog enabled, `portfolio_hr` catalog already exists.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verify catalog

# COMMAND ----------

# DBTITLE 1,Confirm catalog exists
spark.sql("SHOW CATALOGS").filter("catalog = 'portfolio_hr'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Landing Zone — raw file uploads

# COMMAND ----------

# DBTITLE 1,Create landing_zone schema
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS portfolio_hr.landing_zone
    COMMENT 'Landing zone for raw uploaded files — IBM HR Attrition CSV and future datasets'
""")
print("✓ Schema portfolio_hr.landing_zone ready")

# COMMAND ----------

# DBTITLE 1,Create raw_files volume (upload CSVs here)
spark.sql("""
    CREATE VOLUME IF NOT EXISTS portfolio_hr.landing_zone.raw_files
    COMMENT 'External volume for raw CSV uploads — upload IBM HR Attrition file here before Bronze ingestion'
""")
print("✓ Volume portfolio_hr.landing_zone.raw_files ready")
print("  Upload path: /Volumes/portfolio_hr/landing_zone/raw_files/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bronze schema — raw Delta tables

# COMMAND ----------

# DBTITLE 1,Create bronze schema
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS portfolio_hr.bronze
    COMMENT 'Bronze layer — raw ingested Delta tables with audit columns, no transformations'
""")
print("✓ Schema portfolio_hr.bronze ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver schema — cleansed & enriched tables

# COMMAND ----------

# DBTITLE 1,Create silver schema
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS portfolio_hr.silver
    COMMENT 'Silver layer — cleaned and enriched HR data with derived business columns (tenure bands, salary bands, satisfaction labels)'
""")
print("✓ Schema portfolio_hr.silver ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Gold schema — business aggregates

# COMMAND ----------

# DBTITLE 1,Create gold schema
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS portfolio_hr.gold
    COMMENT 'Gold layer — reporting-ready aggregates, fact tables, and flight risk model scores for Power BI and HTML report'
""")
print("✓ Schema portfolio_hr.gold ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify all schemas

# COMMAND ----------

# DBTITLE 1,List all schemas in portfolio_hr
schemas = spark.sql("SHOW SCHEMAS IN portfolio_hr").collect()
print("Schemas in portfolio_hr:")
for s in schemas:
    print(f"  • {s['databaseName']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verify volume and confirm upload path

# COMMAND ----------

# DBTITLE 1,Show volume details
spark.sql("SHOW VOLUMES IN portfolio_hr.landing_zone").display()

# COMMAND ----------

# DBTITLE 1,Print upload instructions
UPLOAD_PATH = "/Volumes/portfolio_hr/landing_zone/raw_files/"

print("=" * 60)
print("SETUP COMPLETE")
print("=" * 60)
print()
print("Upload the following file to:")
print(f"  {UPLOAD_PATH}")
print()
print("  ✓ WA_Fn-UseC_-HR-Employee-Attrition.csv")
print()
print("Source: IBM Watson Analytics — HR Employee Attrition Dataset")
print("Rows: 1,470 employees | Columns: 35")
print()
print("Then run notebooks in order:")
print("  01_bronze_ingestion.py")
print("  02_silver_transform.py")
print("  03_gold_aggregates.py")
print("  04_flight_risk_model.py")
print("  05_cross_tab_aggregates.py")
print("=" * 60)
