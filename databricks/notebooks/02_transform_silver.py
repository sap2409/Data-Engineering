# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze → Silver transform (+ quarantine)
# MAGIC Same cleaning rules as the local `src/transform/bronze_to_silver.py` job.
# MAGIC Writes Delta **silver_*** and **quarantine_*** tables.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "ecommerce_dq")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

EMAIL_PATTERN = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
ALLOWED_STATUS = ["pending", "paid", "shipped", "cancelled", "refunded"]
CUSTOMER_SILVER_COLS = ["customer_id", "name", "email", "country", "signup_date"]
ORDER_SILVER_COLS = ["order_id", "customer_id", "order_date", "amount", "currency", "status"]


def _blank(col: str):
    return F.col(col).isNull() | (F.trim(F.col(col).cast("string")) == "")


def _with_reason(df: DataFrame, reason: str) -> DataFrame:
    return df.withColumn("rejection_reason", F.lit(reason)).withColumn(
        "rejected_at", F.lit(datetime.now(timezone.utc).isoformat())
    )


def _union_quarantine(parts, bronze_cols):
    quarantine = parts[0]
    for part in parts[1:]:
        quarantine = quarantine.unionByName(part, allowMissingColumns=True)
    return quarantine.select(*bronze_cols, "rejection_reason", "rejected_at")


def transform_customers(bronze: DataFrame):
    ordered = bronze.withColumn("_row", F.monotonically_increasing_id())
    ranked = ordered.withColumn(
        "_rn", F.row_number().over(Window.partitionBy("customer_id").orderBy("_row"))
    )
    dupes = _with_reason(ranked.filter(F.col("_rn") > 1), "duplicate_customer_id").drop("_row", "_rn")
    unique = ranked.filter(F.col("_rn") == 1).drop("_row", "_rn")

    bad_email = unique.filter(_blank("email") | ~F.col("email").rlike(EMAIL_PATTERN))
    q_email = _with_reason(bad_email, "invalid_or_blank_email")
    ok_email = unique.filter(~_blank("email") & F.col("email").rlike(EMAIL_PATTERN))

    bad_name = ok_email.filter(_blank("name"))
    q_name = _with_reason(bad_name, "blank_name")
    ok_name = ok_email.filter(~_blank("name"))

    silver = ok_name.withColumn(
        "country",
        F.when(_blank("country"), F.lit("UNKNOWN")).otherwise(F.col("country")),
    ).select(*CUSTOMER_SILVER_COLS)

    quarantine = _union_quarantine([dupes, q_email, q_name], list(bronze.columns))
    return silver, quarantine


def transform_orders(bronze: DataFrame, silver_customers: DataFrame):
    ordered = bronze.withColumn("_row", F.monotonically_increasing_id())
    ranked = ordered.withColumn(
        "_rn", F.row_number().over(Window.partitionBy("order_id").orderBy("_row"))
    )
    dupes = _with_reason(ranked.filter(F.col("_rn") > 1), "duplicate_order_id").drop("_row", "_rn")
    unique = ranked.filter(F.col("_rn") == 1).drop("_row", "_rn")

    bad_amount = unique.filter(
        F.col("amount").isNull() | (F.col("amount").cast("double") <= F.lit(0.0))
    )
    q_amount = _with_reason(bad_amount, "non_positive_or_null_amount")
    ok_amount = unique.filter(
        F.col("amount").isNotNull() & (F.col("amount").cast("double") > F.lit(0.0))
    )

    bad_status = ok_amount.filter(F.col("status").isNull() | ~F.col("status").isin(ALLOWED_STATUS))
    q_status = _with_reason(bad_status, "invalid_status")
    ok_status = ok_amount.filter(F.col("status").isin(ALLOWED_STATUS))

    bad_fk_null = ok_status.filter(_blank("customer_id"))
    q_fk_null = _with_reason(bad_fk_null, "blank_customer_id")
    ok_fk = ok_status.filter(~_blank("customer_id"))

    valid_customers = silver_customers.select("customer_id").distinct()
    orphans = ok_fk.join(valid_customers, "customer_id", "left_anti")
    q_orphans = _with_reason(orphans, "orphan_customer_id")
    ok_ref = ok_fk.join(valid_customers, "customer_id", "inner")

    silver = ok_ref.select(*ORDER_SILVER_COLS)
    quarantine = _union_quarantine(
        [dupes, q_amount, q_status, q_fk_null, q_orphans],
        list(bronze.columns),
    )
    return silver, quarantine

# COMMAND ----------

bronze_customers = spark.table(f"{catalog}.{schema}.bronze_customers")
bronze_orders = spark.table(f"{catalog}.{schema}.bronze_orders")

silver_customers, q_customers = transform_customers(bronze_customers)
silver_orders, q_orders = transform_orders(bronze_orders, silver_customers)

silver_customers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.silver_customers"
)
q_customers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.quarantine_customers"
)
silver_orders.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.silver_orders"
)
q_orders.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.quarantine_orders"
)

summary = [
    ("customers", bronze_customers.count(), silver_customers.count(), q_customers.count()),
    ("orders", bronze_orders.count(), silver_orders.count(), q_orders.count()),
]
display(spark.createDataFrame(summary, ["table", "bronze", "silver", "quarantine"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quarantine reasons (great interview visual)

# COMMAND ----------

display(
    spark.table(f"{catalog}.{schema}.quarantine_customers")
    .groupBy("rejection_reason")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

display(
    spark.table(f"{catalog}.{schema}.quarantine_orders")
    .groupBy("rejection_reason")
    .count()
    .orderBy(F.desc("count"))
)
