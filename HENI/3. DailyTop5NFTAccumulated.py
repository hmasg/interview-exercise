# Databricks notebook source
# MAGIC %md
# MAGIC #### Interview Exercise
# MAGIC <br>
# MAGIC <ol>
# MAGIC   <li>Remove duplicates of the blockchain transaction based on the key columns: transaction_timestamp, from_address, and to_address</li>
# MAGIC   <li>Aggregate the de-duplicated data across all files by NFT_TOKEN to get:
# MAGIC       <ol>
# MAGIC         <li>Daily top-5 NFT tokens with the highest amount</li>
# MAGIC       <li><b>Daily top-5 NFT tokens with the accumulated transaction amounts</b></li>
# MAGIC     </ol>
# MAGIC   </li>
# MAGIC   <li>Write these two aggregated data separately in 2 parquet files with columns:
# MAGIC     <ol>
# MAGIC       <li>date, token_id, amount</li>
# MAGIC       <li>date, token_id, accumulated_amount</li>
# MAGIC     </ol>
# MAGIC   </li>
# MAGIC   <li>The process should be idempotent</li>
# MAGIC   <li>Code reusability is important when writing production pipelines, consider how you would make your code modular and reusable</li>
# MAGIC   <li>Testing is critical for code-quality, consider the best strategy to test your code</li>
# MAGIC </ol>
# MAGIC 
# MAGIC 
# MAGIC ##### Utilities
# MAGIC - <a href="https://greatexpectations.io/expectations/" target="_blank">Great Expectations</a>: `Tool used for data assertions`
# MAGIC - <a href="https://github.com/hmasg/interview-exercise" target="_blank">Github Repo</a>: `Files of the project`

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Environment: Install Libraries
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** `Libraries common to all the notebooks` 

# COMMAND ----------

# MAGIC %run "./Common/Libraries/Libraries"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Environment: Import Libraries

# COMMAND ----------

from pyspark.sql.functions import count, avg, col, max, row_number, to_date,sum
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType
import great_expectations as ge
import json
import numpy as np

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Environment: Variables

# COMMAND ----------

# destination folder
DailyTop5NFT_output_folder = "/FileStore/heni/result/DailyTop5NFTAccumulated/"
# source and destination file type
file_type = "parquet"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Environment: Automatic optimization
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> `Adaptive Query Execution and Dynamic Partition` 

# COMMAND ----------

# MAGIC %run "./Common/Optimization/Optimization"

# COMMAND ----------

# MAGIC %md
# MAGIC #### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Development: Load source data
# MAGIC 
# MAGIC | Type         | Path |
# MAGIC |-----------------|-----------------|
# MAGIC | `Source`         | `/FileStore/heni/source/data.parquet` |
# MAGIC | `Output`    | `/FileStore/heni/result/DailyTop5NFTAccumulated/DailyTop5NFTAccumulated.parquet` |

# COMMAND ----------

# Move file to the HENI folder
try:
  dbutils.fs.mv("/FileStore/tables/data.parquet", "/FileStore/heni/source/data.parquet")
except Exception as e:
  print("Folder don't exist")

# File location 
file_location = "/FileStore/heni/source/data.parquet"

# Load the data in a dataframe
df = spark.read.format(file_type) \
  .load(file_location)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Development: Remove duplicated rows

# COMMAND ----------

df_no_duplicates = df.withColumn("date",to_date("transaction_timestamp")).distinct()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Development: Find the accumulated amount for each token by date

# COMMAND ----------

df_DailyTop5NFT_stage01= df_no_duplicates.select("date","token_id","amount") \
                      .groupBy("date","token_id").agg(sum("amount")) \
                      .sort(col("date"),col("sum(amount)").desc())

df_DailyTop5NFT_stage01.display()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Development: Using the function WINDOW to partitioning the dataset by date
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** `PySpark Window functions are used to calculate results such as the rank, row number etc over a range of input rows.` 

# COMMAND ----------

windowDay = Window.partitionBy("date").orderBy(col("sum(amount)").desc())
df_DailyTop5NFT_stage02 = df_DailyTop5NFT_stage01.withColumn("row",row_number().over(windowDay))
df_DailyTop5NFT = df_DailyTop5NFT_stage02.select("date","token_id", col("sum(amount)").alias("amount")).filter(col("row") <= 5)
df_DailyTop5NFT.display()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Data Quality: Create Great-Expectations Object

# COMMAND ----------

df_ge = ge.dataset.SparkDFDataset(df_DailyTop5NFT)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Data Quality: Assertions

# COMMAND ----------

validation_amout_double_type = json.loads(str(df_ge.expect_column_values_to_be_of_type("amount","DoubleType")))['success']
validation_column_tokenID_exist = json.loads(str(df_ge.expect_column_to_exist("token_id")))['success']
validation_number_of_records = json.loads(str(df_ge.expect_table_row_count_to_equal(195)))['success']

sucess = True

if validation_amout_double_type == False:
  sucess = False
  
if validation_column_tokenID_exist == False:
  sucess = False
  
if validation_number_of_records == False:
  sucess = False

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Develoment: Save results

# COMMAND ----------

if sucess==True: 
  file_name = "DailyTop5NFTAccumulated"
  (df_DailyTop5NFT
   .coalesce(1)
   .write
   .mode("overwrite")
   .format("com.databricks.spark.parquet")
   .parquet(DailyTop5NFT_output_folder))
  
  folders = np.array([DailyTop5NFT_output_folder])
  for folder in folders:
    try:
      files = dbutils.fs.ls(folder)
      for file in files:
        if file.name.startswith("part-")==False:
          if file.isDir()==False:
            dbutils.fs.rm(file[0])
        else:
          dbutils.fs.mv(file.path, "%s/%s.parquet" % (folder,file_name))
    except Exception as e:
      print("Folder don't exist")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Validation: Verify the file was saved

# COMMAND ----------

fslsSchema = StructType(
  [
    StructField('path', StringType()),
    StructField('name', StringType()),
    StructField('size', LongType()),
    StructField('modtime', LongType())
  ]
)

filelist = dbutils.fs.ls(DailyTop5NFT_output_folder)
df_files = spark.createDataFrame(filelist, fslsSchema)
df_files.select("path","name").display()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Validation: Open file

# COMMAND ----------

file_location=df_files.first()['path']
df_comprobation = spark.read.format(file_type).load(file_location)
df_comprobation.display()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check number of records
# MAGIC 
# MAGIC Verify that you wrote the parquet file out to **destFile** and that you have the right number of records.

# COMMAND ----------

parquetFiles = len(list(filter(lambda f: f.path.endswith(".parquet"), dbutils.fs.ls(file_location))))

df_final = spark.read.parquet(file_location)
finalCount = df_final.count()

assert parquetFiles == 1, "expected 1 parquet files located in destFile"
assert finalCount == 195, "expected 195 records in finalDF"