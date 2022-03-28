# Databricks notebook source
# MAGIC %md
# MAGIC <img src = "https://docs.delta.io/latest/_static/delta-lake-logo.png" width=600>
# MAGIC 
# MAGIC </br>
# MAGIC  ### Change Data Feed
# MAGIC 
# MAGIC The Delta change data feed represents row-level changes between versions of a Delta table. When enabled on a Delta table, the runtime records “change events” for all the data written into the table. This includes the row data along with metadata indicating whether the specified row was inserted, deleted, or updated.
# MAGIC </br>
# MAGIC 
# MAGIC <img src = "https://databricks.com/wp-content/uploads/2021/06/How-to-Simplify-CDC-with-Delta-Lakes-Change-Data-Feed-blog-image6.jpg" width=1000>

# COMMAND ----------

# MAGIC %md ### Create a silver table that contains a list of addresses
# MAGIC 
# MAGIC We are going to use this table to simulate appends and updates commands that are  common for transactional workloads.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS cdf CASCADE;
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS cdf;
# MAGIC 
# MAGIC USE cdf;

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS cdf.silverTable;
# MAGIC CREATE TABLE cdf.silverTable(
# MAGIC   primaryKey int,
# MAGIC   address string,
# MAGIC   current boolean,
# MAGIC   effectiveDate string,
# MAGIC   endDate string
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %md ### Enable Change Data Feed on the Silver Table
# MAGIC 
# MAGIC This will allow us to capture the row level changes that are made to directly to the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE cdf.silverTable SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES cdf.silverTable

# COMMAND ----------

# MAGIC %md ### Insert Data into Silver Table

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT into cdf.silverTable
# MAGIC select 11 primaryKey, "A new customer address" as address, true as current, "2021-10-27" as effectiveDate, null as endDate
# MAGIC union
# MAGIC select 12 primaryKey, "A different address" as address, true as current, "2021-10-27" as effectiveDate, null as endDate
# MAGIC union
# MAGIC select 13 primaryKey, "A another different address" as address, true as current, "2021-10-27" as effectiveDate, null as endDate;
# MAGIC 
# MAGIC SELECT * FROM cdf.silverTable

# COMMAND ----------

# MAGIC %md ### Upsert new Data into Silver Table
# MAGIC 
# MAGIC This include an update to an existing address, and also a brand new address.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW updates
# MAGIC as
# MAGIC select 11 primaryKey, "A updated address" as address, true as current, "2021-10-27" as effectiveDate, null as endDate
# MAGIC union
# MAGIC select 99 primaryKey, "A completely new address" as address, true as current, "2021-10-27" as effectiveDate, null as endDate;
# MAGIC 
# MAGIC SELECT * FROM updates;

# COMMAND ----------

# MAGIC %md
# MAGIC We want to merge the view into the silver table. Specifically if the address already exists we want to set the `endDate` of the old record to be the `effectiveDate` of the new address record and change the flag for the current column to `false`. We then want to append the new update address as a brand new row. For completely new addresses we want to insert this as a new row.

# COMMAND ----------

# MAGIC %sql 
# MAGIC MERGE INTO cdf.silverTable as original USING (
# MAGIC   select
# MAGIC     updates.primaryKey as merge,
# MAGIC     updates.*
# MAGIC   FROM
# MAGIC     updates
# MAGIC   UNION ALL
# MAGIC   SELECT
# MAGIC     null as merge,
# MAGIC     updates.*
# MAGIC   FROM
# MAGIC     updates
# MAGIC     INNER JOIN cdf.silverTable original on updates.primaryKey = original.primaryKey
# MAGIC   where
# MAGIC     original.current = true
# MAGIC ) mergedupdates on original.primaryKey = mergedUpdates.merge
# MAGIC WHEN MATCHED
# MAGIC and original.current = true THEN
# MAGIC UPDATE
# MAGIC set
# MAGIC   current = false,
# MAGIC   endDate = mergedupdates.effectiveDate
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cdf.silverTable

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history cdf.silverTable

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes('cdf.silverTable',2,3) order by _commit_timestamp desc

# COMMAND ----------

# MAGIC %python
# MAGIC changes_df = spark.read.format("delta").option("readChangeData", True).option("startingVersion", 2).option("endingversion", 3).table('cdf.silverTable')
# MAGIC display(changes_df)

# COMMAND ----------

# MAGIC %md ### Generate Gold table and propagate changes
# MAGIC 
# MAGIC In some cases we may not want to show each data at the transaction level, and want present to users a high level aggregate. In this case we can use CDF to make sure that the changes are propaged effieciently without having to merge large amounts of data

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS cdf.goldTable;
# MAGIC CREATE TABLE cdf.goldTable(
# MAGIC   primaryKey int,
# MAGIC   address string
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Collect only the latest version for address
# MAGIC CREATE OR REPLACE TEMPORARY VIEW silverTable_latest_version as
# MAGIC SELECT * 
# MAGIC     FROM 
# MAGIC          (SELECT *, rank() over (partition by primaryKey order by _commit_version desc) as rank
# MAGIC           FROM table_changes('silverTable',2,3)
# MAGIC           WHERE _change_type ='insert')
# MAGIC     WHERE rank=1;
# MAGIC     
# MAGIC SELECT * FROM silverTable_latest_version

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Merge the changes to gold
# MAGIC MERGE INTO cdf.goldTable t USING silverTable_latest_version s ON s.primaryKey = t.primaryKey
# MAGIC         WHEN MATCHED THEN UPDATE SET address = s.address
# MAGIC         WHEN NOT MATCHED THEN INSERT (primarykey, address) VALUES (s.primarykey, s.address)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cdf.goldTable

# COMMAND ----------

# MAGIC %md ### Example that Combines Snapshots with Change Data Feed

# COMMAND ----------

# MAGIC %md #### Create an intial dataset and save this as a Delta table. 
# MAGIC 
# MAGIC This will be source table we'll use to propogate changes downstream.

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS cdf.example_source;

# COMMAND ----------

countries = [("USA", 10000, 20000), ("India", 1000, 1500), ("UK", 7000, 10000), ("Canada", 500, 700) ]
columns = ["Country","NumVaccinated","AvailableDoses"]

spark.createDataFrame(data=countries, schema = columns).write \
                .format("delta") \
                .mode("overwrite") \
                .option("userMetadata", "Snapshot Example 1") \
                .saveAsTable("cdf.example_source") \

streaming_silverTable_df = spark.read.format("delta").table("cdf.example_source")
streaming_silverTable_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.commitInfo.userMetadata =;
# MAGIC ALTER TABLE cdf.example_source SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.commitInfo.userMetadata =;
# MAGIC UPDATE cdf.example_source SET NumVaccinated = 1000, AvailableDoses = 200 WHERE COUNTRY = 'Canada';
# MAGIC UPDATE cdf.example_source SET NumVaccinated = 2000, AvailableDoses = 500 WHERE COUNTRY = 'India';
# MAGIC 
# MAGIC SELECT * FROM cdf.example_source

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history cdf.example_source

# COMMAND ----------

# MAGIC %md Let's do a few more operations...

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM cdf.example_source where Country = 'UK';
# MAGIC SELECT * FROM cdf.example_source;

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.commitInfo.userMetadata =;
# MAGIC INSERT into cdf.example_source
# MAGIC SELECT "France" Country, 7500 as NumVacinated, 5000 as AvailableDoses;
# MAGIC UPDATE cdf.example_source SET NumVaccinated = 1200, AvailableDoses = 0 WHERE COUNTRY = 'CANADA';
# MAGIC 
# MAGIC SELECT * FROM cdf.example_source

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.commitInfo.userMetadata =Snapshot Example 2;
# MAGIC INSERT into cdf.example_source
# MAGIC SELECT "Mexico" Country, 2000 as NumVacinated, 1000 as AvailableDoses;
# MAGIC 
# MAGIC SELECT * FROM cdf.example_source

# COMMAND ----------

# MAGIC %md #### Let's set up what the workflow might look like for a consumer.
# MAGIC This will first retrieve a point in time snapshot of the source table, then starts subscribing to incremental updates using Spark Structured Streaming and CDF.

# COMMAND ----------

# MAGIC %md # Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS cdf.example_source;
# MAGIC DROP TABLE IF EXISTS cdf.example_sink;
