# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "69118442-f38c-4f27-ac49-50e1498a489b",
# META       "default_lakehouse_name": "New_Lake",
# META       "default_lakehouse_workspace_id": "84782802-80aa-4b18-8e26-c94ddb42e9a6",
# META       "known_lakehouses": [
# META         {
# META           "id": "69118442-f38c-4f27-ac49-50e1498a489b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================
# Cell 1: Imports +)
# ============================================
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import col
from datetime import datetime

dq_log_schema = StructType([
    StructField("SourceType", StringType(), True),
    StructField("TableName", StringType(), True),
    StructField("CheckName", StringType(), True),
    StructField("CheckResult", StringType(), True),   # Pass / Fail / Skip
    StructField("Details", StringType(), True),
    StructField("RowsAffected", IntegerType(), True),
    StructField("RunDate", TimestampType(), True)
])

def _ensure_log_table():
    if not spark.catalog.tableExists("DataQualityLog"):
        empty = spark.createDataFrame([], schema=dq_log_schema)
        empty.write.format("delta").mode("overwrite").saveAsTable("DataQualityLog")
        print("✅ DataQualityLog table created successfully!")

def log_result(results_list, source_type, table_name, check_name, result, details, rows_affected=0):
    results_list.append(Row(
        SourceType=source_type, TableName=table_name, CheckName=check_name,
        CheckResult=result, Details=details, RowsAffected=int(rows_affected),
        RunDate=datetime.now()
    ))
    icon = "✅" if result == "Pass" else ("⏭️" if result == "Skip" else "⚠️")
    print(f"{icon} [{source_type}] {table_name} | {check_name}: {result} - {details}")

# 
_ensure_log_table()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# Cell 2: ⚙️ CONFIG
# ============================================

VALIDATION_CONFIG = {
    "Excel": {
        "Bronze_FakeStore_Orders":       {"key_columns": ["OrderID", "CustomerID"], "non_negative_columns": ["Freight"], "check_odata_leftover": False},
        "Bronze_FakeStore_OrderDetails": {"key_columns": ["OrderID", "ProductID"], "non_negative_columns": ["Quantity", "UnitPrice"], "check_odata_leftover": False},
        "Bronze_FakeStore_Customers":    {"key_columns": ["CustomerID"], "non_negative_columns": [], "check_odata_leftover": False},
        "Bronze_FakeStore_Products":     {"key_columns": ["ProductID"], "non_negative_columns": ["UnitPrice", "UnitsInStock"], "check_odata_leftover": False},
    },
    "API": {
        "Categories":    {"key_columns": ["CategoryID"], "non_negative_columns": [], "check_odata_leftover": True},
        "Customers":     {"key_columns": ["CustomerID"], "non_negative_columns": [], "check_odata_leftover": True},
        "Employees":     {"key_columns": ["EmployeeID"], "non_negative_columns": [], "check_odata_leftover": True},
        "Order_Details": {"key_columns": ["OrderID", "ProductID"], "non_negative_columns": ["Quantity", "UnitPrice"], "check_odata_leftover": True},
        "Orders":        {"key_columns": ["OrderID", "CustomerID", "EmployeeID"], "non_negative_columns": ["Freight"], "check_odata_leftover": True},
        "Products":      {"key_columns": ["ProductID", "CategoryID", "SupplierID"], "non_negative_columns": ["UnitPrice"], "check_odata_leftover": True},
        "Suppliers":     {"key_columns": ["SupplierID"], "non_negative_columns": [], "check_odata_leftover": True},
        "Shippers":      {"key_columns": ["ShipperID"], "non_negative_columns": [], "check_odata_leftover": True},
        "Regions":       {"key_columns": ["RegionID"], "non_negative_columns": [], "check_odata_leftover": True},
        "Territories":   {"key_columns": ["TerritoryID"], "non_negative_columns": [], "check_odata_leftover": True},
    }
}
#
DEFAULT_NUMERIC_KEYWORDS = ["Price", "Quantity", "Discount", "Freight", "Stock"]
#
REFERENTIAL_CHECKS = {
    "API": [
        ("Order_Details", "Orders", "OrderID"),
        ("Order_Details", "Products", "ProductID"),
        ("Orders", "Customers", "CustomerID"),
        ("Orders", "Employees", "EmployeeID"),
        ("Products", "Categories", "CategoryID"),
        ("Products", "Suppliers", "SupplierID"),
    ],
    "Excel": [
        ("Bronze_OrderDetails_Excel", "Bronze_Orders_Excel", "OrderID"),
        ("Bronze_OrderDetails_Excel", "Bronze_Products_Excel", "ProductID"),
        ("Bronze_Orders_Excel", "Bronze_Customers_Excel", "CustomerID"),
    ]
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



def check_row_count(df):
    count = df.count()
    result = "Pass" if count > 0 else "Fail"
    return ("RowCountCheck", result, f"{count} rows found", count)


def check_nulls_in_key_columns(df, key_columns):
    issues, total_nulls = [], 0
    for c in key_columns:
        if c in df.columns:
            n = df.filter(col(c).isNull()).count()
            if n > 0:
                issues.append(f"{c}: {n} nulls")
                total_nulls += n
        else:
            issues.append(f"{c}: column not found")
    result = "Fail" if issues else "Pass"
    return ("NullCheck", result, "; ".join(issues) if issues else "No nulls in key columns", total_nulls)


def check_duplicates(df):
   
    total = df.count()
    distinct = df.dropDuplicates().count()
    dup = total - distinct
    result = "Fail" if dup > 0 else "Pass"
    return ("DuplicateCheck", result, f"{dup} duplicate rows found (not removed here)", dup)


def check_numeric_types(df, numeric_keywords):
    issues = []
    for f in df.schema.fields:
        if any(k.lower() in f.name.lower() for k in numeric_keywords) and "String" in str(f.dataType):
            issues.append(f"{f.name} is String but expected numeric")
    result = "Fail" if issues else "Pass"
    return ("TypeCheck", result, "; ".join(issues) if issues else "Numeric columns OK", len(issues))


def check_negative_values(df, non_negative_columns):
    issues, total_bad = [], 0
    for c in non_negative_columns:
        if c in df.columns:
            n = df.filter(col(c) < 0).count()
            if n > 0:
                issues.append(f"{c}: {n} negative values")
                total_bad += n
    result = "Fail" if issues else "Pass"
    return ("NegativeValueCheck", result, "; ".join(issues) if issues else "No negative values", total_bad)


def check_odata_leftovers(df):
    has_odata = any("@odata" in c for c in df.columns)
    has_prefix = any(c.startswith("value.") for c in df.columns)
    result = "Fail" if (has_odata or has_prefix) else "Pass"
    details = "Leftover @odata / value. columns found" if (has_odata or has_prefix) else "No leftover metadata columns"
    return ("OdataLeftoverCheck", result, details, 0)


def check_referential_integrity(child_df, parent_df, key_col):
    if key_col not in child_df.columns or key_col not in parent_df.columns:
        return ("ReferentialIntegrityCheck", "Skip", f"Column {key_col} missing in one of the tables", 0)
    orphans = child_df.join(parent_df, key_col, "left_anti").count()
    result = "Fail" if orphans > 0 else "Pass"
    return ("ReferentialIntegrityCheck", result, f"{orphans} orphan rows on {key_col}", orphans)


def run_checks_for_table(results_list, source_type, table_name, table_cfg):
   
    try:
        df = spark.read.table(table_name)
    except Exception as e:
        log_result(results_list, source_type, table_name, "TableRead", "Fail", str(e))
        return

    for fn_name, res, det, rows in [
        check_row_count(df),
        check_nulls_in_key_columns(df, table_cfg.get("key_columns", [])),
        check_duplicates(df),
        check_numeric_types(df, DEFAULT_NUMERIC_KEYWORDS),
        check_negative_values(df, table_cfg.get("non_negative_columns", [])),
    ]:
        log_result(results_list, source_type, table_name, fn_name, res, det, rows)

    if table_cfg.get("check_odata_leftover"):
        fn_name, res, det, rows = check_odata_leftovers(df)
        log_result(results_list, source_type, table_name, fn_name, res, det, rows)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


_ensure_log_table()
all_results = []

for source_type, tables in VALIDATION_CONFIG.items():
    print(f"\n--- Validating source: {source_type} ---")
    for table_name, table_cfg in tables.items():
        run_checks_for_table(all_results, source_type, table_name, table_cfg)

for source_type, checks in REFERENTIAL_CHECKS.items():
    print(f"\n--- Referential integrity: {source_type} ---")
    for child_table, parent_table, key_col in checks:
        try:
            child_df = spark.read.table(child_table)
            parent_df = spark.read.table(parent_table)
            fn_name, res, det, rows = check_referential_integrity(child_df, parent_df, key_col)
            log_result(all_results, source_type, f"{child_table}->{parent_table}", fn_name, res, det, rows)
        except Exception as e:
            log_result(all_results, source_type, f"{child_table}->{parent_table}", "ReferentialIntegrityCheck", "Fail", str(e))

if all_results:
    results_df = spark.createDataFrame(all_results, schema=dq_log_schema)
    results_df.write.format("delta").mode("append").saveAsTable("DataQualityLog")

print("\n✅ Validation run complete, results appended to DataQualityLog")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql.functions import max as spark_max
import datetime as _dt

log_df = spark.read.table("DataQualityLog")
latest_run = log_df.agg(spark_max("RunDate")).collect()[0][0]
print(f"آخر تشغيلة: {latest_run}\n")

log_df.filter(col("RunDate") >= latest_run - _dt.timedelta(minutes=30)) \
      .groupBy("SourceType", "CheckResult").count().orderBy("SourceType", "CheckResult").show()

failed_now = log_df.filter((col("CheckResult") == "Fail") & (col("RunDate") >= latest_run - _dt.timedelta(minutes=30)))
if failed_now.count() > 0:
    print("⚠️ need to review it  :")
    failed_now.select("SourceType", "TableName", "CheckName", "Details").show(truncate=False)
else:
    print("✅ All Chechecd Confirmed ")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
