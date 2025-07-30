import dlt
import pyspark.sql.functions as F

source = spark.conf.get("source")


## A. Create the bronze streaming table in your labuser.1_bronze_db schema from a JSON files in your volume
  # NOTE: read_files references the 'source' configuration key from your DLT pipeline settings. 
  # NOTE: 'source' = '/Volumes/dbacademy/ops/your-labuser-name'
@dlt.table(name = "1_bronze_db.status_bronze_demo6",
           comment = "Ingest raw JSON order status files from cloud storage",
           table_properties = {
                            "quality":"bronze",
                            "pipelines.reset.allowed":"false"
                        })
def status_bronze_demo6():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", True)
            .load(f"{source}/status")
            .select(
                "*",
                F.current_timestamp().alias("processing_time"), 
                "_metadata.file_name"
            )
    )


## B. Create the silver streaming table in your labuser.2_silver_db schema (database)
@dlt.table(name = "2_silver_db.status_silver_demo6",
           comment = "Order with each status and timestamp",
           table_properties = {
                            "quality":"silver"
                        })

# Expectations
@dlt.expect_or_drop("valid_timestamp", "order_status_timestamp > '2021-12-25'")
@dlt.expect("valid_order_status", "order_status IN ('on the way','canceled','return canceled','delivered','return processed','placed','preparing')")

def status_silver_demo5():
    return (
        dlt.read_stream("1_bronze_db.status_bronze_demo6")
            .select(
                "order_id",
                "order_status",
                F.col("status_timestamp").cast("timestamp").alias("order_status_timestamp")
            )
    )


## C. Use a Materialized View to Join Two Streaming Tables
@dlt.table(
    name = "3_gold_db.full_order_info_gold_demo6",
    comment = "Joining the orders and order status silver tables to view all orders with each individual status per order",
    table_properties = {
                        "quality":"gold"
                    })
def full_order_info_gold_demo6():
    return (
        dlt
        .read("2_silver_db.status_silver_demo6").alias("status")
        .join(dlt.read("2_silver_db.orders_silver_demo6").alias("orders"), on = "order_id", how = "inner")
        .select(
            "orders.order_id",
            "orders.order_timestamp",
            "status.order_status",
            "status.order_status_timestamp"
        )
    )


##
## D. Create Materialized Views for Cancelled and Delivered Orders
##

## CANCELLED ORDERS
@dlt.table(
    name="3_gold_db.cancelled_orders_gold_demo6",
    comment="All cancelled orders",
    table_properties={
        "quality": "gold"
    }
)
def cancelled_orders_gold_demo5():
    full_orders = dlt.read("3_gold_db.full_order_info_gold_demo6")
    return (
        full_orders
        .filter(F.col("order_status") == "canceled")
        .select(
            "order_id",
            "order_timestamp",
            "order_status",
            "order_status_timestamp",
            F.datediff("order_status_timestamp", "order_timestamp").alias("days_to_cancel")
        )
    )


## DELIVERED ORDERS
@dlt.table(
    name="3_gold_db.delivered_orders_gold_demo6",
    comment="All delivered orders",
    table_properties={
        "quality": "gold"
    }
)
def delivered_orders_gold_demo6():
    full_orders = dlt.read("3_gold_db.full_order_info_gold_demo6")
    return (
        full_orders
        .filter(F.col("order_status") == "delivered")
        .select(
            "order_id",
            "order_timestamp",
            "order_status",
            "order_status_timestamp",
            F.datediff("order_status_timestamp", "order_timestamp").alias("days_to_delivery")
        )
    )