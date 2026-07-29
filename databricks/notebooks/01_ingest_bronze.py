# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest Bronze
# MAGIC Loads intentionally dirty ecommerce sample data into Bronze Delta tables.
# MAGIC No file upload required — data is embedded for a fast interview demo.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "ecommerce_dq")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

from pyspark.sql import Row

customers = [
    Row(customer_id="C001", name="Alice Sharma", email="alice.sharma@example.com", country="IN", signup_date="2024-01-10", raw_source="crm"),
    Row(customer_id="C002", name="Bob Chen", email="bob.chen@example.com", country="US", signup_date="2024-01-12", raw_source="crm"),
    Row(customer_id="C003", name="Cara Lopez", email="cara.lopez@example.com", country="MX", signup_date="2024-01-15", raw_source="web"),
    Row(customer_id="C004", name="Dan Patel", email=None, country="IN", signup_date="2024-02-01", raw_source="crm"),
    Row(customer_id="C005", name="Eve Okonkwo", email="eve.okonkwo@example.com", country="NG", signup_date="2024-02-03", raw_source="web"),
    Row(customer_id="C006", name="Frank Mueller", email="frank.mueller@example.com", country="DE", signup_date="2024-02-05", raw_source="crm"),
    Row(customer_id="C002", name="Bob Chen", email="bob.chen@example.com", country="US", signup_date="2024-01-12", raw_source="crm"),  # duplicate
    Row(customer_id="C007", name="Grace Kim", email="not-an-email", country="KR", signup_date="2024-02-10", raw_source="web"),
    Row(customer_id="C008", name="Hiro Tanaka", email="hiro.tanaka@example.com", country="JP", signup_date="2024-02-12", raw_source="crm"),
    Row(customer_id="C009", name=None, email="iris.novak@example.com", country="CZ", signup_date="2024-02-14", raw_source="web"),
    Row(customer_id="C010", name="James Brown", email="james.brown@example.com", country="US", signup_date="2024-02-20", raw_source="crm"),
    Row(customer_id="C011", name="Kelly Wong", email="kelly.wong@example.com", country=None, signup_date="2024-03-01", raw_source="web"),
    Row(customer_id="C012", name="Liam OBrien", email="liam.obrien@example.com", country="IE", signup_date="2024-03-05", raw_source="crm"),
]

orders = [
    Row(order_id="O1001", customer_id="C001", order_date="2024-03-01", amount=120.50, currency="INR", status="paid", ingested_at="2024-03-02T01:00:00"),
    Row(order_id="O1002", customer_id="C002", order_date="2024-03-02", amount=45.00, currency="USD", status="shipped", ingested_at="2024-03-03T01:00:00"),
    Row(order_id="O1003", customer_id="C003", order_date="2024-03-03", amount=-10.00, currency="MXN", status="paid", ingested_at="2024-03-04T01:00:00"),
    Row(order_id="O1004", customer_id="C005", order_date="2024-03-04", amount=200.00, currency="NGN", status="pending", ingested_at="2024-03-05T01:00:00"),
    Row(order_id="O1005", customer_id="C006", order_date="2024-03-05", amount=89.99, currency="EUR", status="paid", ingested_at="2024-03-06T01:00:00"),
    Row(order_id="O1006", customer_id="C008", order_date="2024-03-06", amount=1500.0, currency="JPY", status="shipped", ingested_at="2024-03-07T01:00:00"),
    Row(order_id="O1007", customer_id="C010", order_date="2024-03-07", amount=None, currency="USD", status="paid", ingested_at="2024-03-08T01:00:00"),
    Row(order_id="O1008", customer_id="C012", order_date="2024-03-08", amount=75.25, currency="EUR", status="cancelled", ingested_at="2024-03-09T01:00:00"),
    Row(order_id="O1002", customer_id="C002", order_date="2024-03-02", amount=45.00, currency="USD", status="shipped", ingested_at="2024-03-03T01:00:00"),  # duplicate
    Row(order_id="O1009", customer_id="C999", order_date="2024-03-09", amount=30.00, currency="USD", status="paid", ingested_at="2024-03-10T01:00:00"),  # orphan
    Row(order_id="O1010", customer_id="C001", order_date="2024-03-10", amount=55.00, currency="INR", status="UNKNOWN", ingested_at="2024-03-11T01:00:00"),
    Row(order_id="O1011", customer_id="C005", order_date="2024-03-11", amount=12.00, currency="NGN", status="refunded", ingested_at="2024-03-12T01:00:00"),
    Row(order_id="O1012", customer_id=None, order_date="2024-03-12", amount=99.00, currency="USD", status="paid", ingested_at="2024-03-13T01:00:00"),
]

bronze_customers = spark.createDataFrame(customers)
bronze_orders = spark.createDataFrame(orders)

bronze_customers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.bronze_customers")
bronze_orders.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.bronze_orders")

print(f"bronze_customers rows: {bronze_customers.count()}")
print(f"bronze_orders rows:    {bronze_orders.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Peek at dirty Bronze (demo talking points)

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.bronze_customers"))

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.bronze_orders"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Issues planted for the demo**
# MAGIC - Duplicate `customer_id` / `order_id`
# MAGIC - Blank / invalid emails, blank names
# MAGIC - Negative / null amounts, invalid `status`
# MAGIC - Orphan `customer_id=C999`, blank FK
