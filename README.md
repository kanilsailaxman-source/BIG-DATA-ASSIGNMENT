# Import necessary PySpark functions
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, DateType, TimestampType

# ----------------- CONFIGURATION (CODE FIX) -----------------
# FIX: Define a single, correct base path for both input and output files.
# The placeholder 'volume_path' has been removed and replaced with this base_path.
base_path = "/Volumes/workspace/internalba/salon&spa/"

# Define paths for input files using the correct base path
path_appointments = f"{base_path}appointments.csv"
path_customers = f"{base_path}customers.csv"
path_salons = f"{base_path}salons.csv"
path_services = f"{base_path}services.csv"

# Define the base path for saving the output files in a new 'outputs' sub-folder
output_path_base = f"{base_path}outputs/"

# ----------------- LOAD DATA -----------------
# Read the CSV files into PySpark DataFrames
appointments_df = spark.read.option("header", "true").option("inferSchema", "true").csv(path_appointments)
customers_df = spark.read.option("header", "true").option("inferSchema", "true").csv(path_customers)
salons_df = spark.read.option("header", "true").option("inferSchema", "true").csv(path_salons)
services_df = spark.read.option("header", "true").option("inferSchema", "true").csv(path_services)

# ----------------- DATA PREPARATION -----------------
# Rename the ambiguous 'city' columns before joining to avoid errors.
customers_df = customers_df.withColumnRenamed("city", "customer_city")
salons_df = salons_df.withColumnRenamed("city", "salon_city")

# Create a master DataFrame by joining all the tables together
master_df = appointments_df.join(customers_df, "customer_id", "left") \
                           .join(salons_df, "salon_id", "left") \
                           .join(services_df, "service_id", "left")

# Filter out cancelled appointments for revenue and trend analysis
completed_appointments_df = master_df.filter(F.col("status") == "Completed")

print("## Master DataFrame created successfully. ##")
display(master_df)

# ----------------- 5 KEY BUSINESS ANALYSES -----------------

# --- 1. Popular Services ---
popular_services_df = completed_appointments_df.groupBy("service_name") \
    .agg(F.count("appointment_id").alias("number_of_appointments")) \
    .orderBy(F.col("number_of_appointments").desc())

print("\n## 1. Popular Services ##")
display(popular_services_df)

# --- 2. Peak Times ---
peak_times_df = completed_appointments_df.withColumn("appointment_hour", F.hour(F.to_timestamp("appointment_time", "HH:mm"))) \
    .groupBy("appointment_hour") \
    .agg(F.count("appointment_id").alias("number_of_appointments")) \
    .orderBy("appointment_hour")

print("\n## 2. Peak Times ##")
display(peak_times_df)

# --- 3. Revenue by Service ---
revenue_by_service_df = completed_appointments_df.groupBy("service_name") \
    .agg(F.round(F.sum("amount_spent"), 2).alias("total_revenue")) \
    .orderBy(F.col("total_revenue").desc())

print("\n## 3. Revenue by Service ##")
display(revenue_by_service_df)

# --- 4. Customer Loyalty ---
customer_loyalty_df = completed_appointments_df.groupBy("customer_name") \
    .agg(F.count("appointment_id").alias("number_of_visits")) \
    .filter(F.col("number_of_visits") > 1) \
    .orderBy(F.col("number_of_visits").desc())

print("\n## 4. Customer Loyalty (Repeat Customers) ##")
display(customer_loyalty_df)

# --- 5. Salon Performance ---
# Use the new 'salon_city' column in the groupBy operation.
salon_performance_df = completed_appointments_df.groupBy("salon_name", "salon_city") \
    .agg(
        F.count("appointment_id").alias("total_appointments"),
        F.round(F.sum("amount_spent"), 2).alias("total_revenue")
    ) \
    .orderBy(F.col("total_revenue").desc())
    
print("\n## 5. Salon Performance ##")
display(salon_performance_df)

# ----------------- SAVE OUTPUTS -----------------
# This part will now work correctly by writing to the valid output path.
popular_services_df.coalesce(1).write.mode("overwrite").option("header","true").csv(output_path_base + "popular_services")
peak_times_df.coalesce(1).write.mode("overwrite").option("header","true").csv(output_path_base + "peak_times")
revenue_by_service_df.coalesce(1).write.mode("overwrite").option("header","true").csv(output_path_base + "revenue_by_service")
customer_loyalty_df.coalesce(1).write.mode("overwrite").option("header","true").csv(output_path_base + "customer_loyalty")
salon_performance_df.coalesce(1).write.mode("overwrite").option("header","true").csv(output_path_base + "salon_performance")

print(f"\n✅ All output files have been successfully saved to your Volume at: {output_path_base}")
