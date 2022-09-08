# Databricks notebook source
# MAGIC %md
# MAGIC #### Interview Exercise
# MAGIC <br>
# MAGIC <ol>
# MAGIC   <li><b>Remove duplicates of the blockchain transaction based on the key columns: transaction_timestamp, from_address, and to_address</b></li>
# MAGIC   <li>Aggregate the de-duplicated data across all files by NFT_TOKEN to get:
# MAGIC       <ol>
# MAGIC       <li>Daily top-5 NFT tokens with the highest amount</li>
# MAGIC       <li>Daily top-5 NFT tokens with the accumulated transaction amounts</li>
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
# MAGIC ##### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Import Libraries

# COMMAND ----------

from pyspark.sql.functions import count, avg, col, max, row_number, to_date, sum
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Automatic optimization
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> `Adaptive Query Execution and Dynamic Partition` 

# COMMAND ----------

# MAGIC %run "./Common/Optimization/Optimization"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Functions

# COMMAND ----------

# MAGIC %run "./Common/Functions/Functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Load source data
# MAGIC 
# MAGIC | Type         | Path |
# MAGIC |-----------------|-----------------|
# MAGIC | `Source`         | `/FileStore/heni/source/data.parquet` |

# COMMAND ----------

# File location and type
file_location = "/FileStore/heni/source/data.parquet"
file_type = "parquet"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .load(file_location)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Number of records on origin

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) First Approach: Duplicated rows based on all the columns
# MAGIC 
# MAGIC 
# MAGIC In this approach we look for records where all the correspondant columns are equal

# COMMAND ----------

df_duplicates_total=df.groupBy("transaction_timestamp","from_address", "to_address","token_id","amount").count().filter("count > 1")
df_duplicates_total.display()
#df1.drop('count').show()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> `Comprobation by the token id = 1116` 

# COMMAND ----------

df.where((df.transaction_timestamp =='2022-06-17T15:46:44.000+0000') & 
          (df.from_address=='0x31837aaf36961274a04b915697fdfca1af31a0c7') &
          (df.to_address=='0x147f9fbd2a056b6316c91966c631c9951ef9348b') &
          (df.token_id=='1116') &
          (df.amount=='39.36375')
         ).display()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> `509-136= 373 duplicated records` 

# COMMAND ----------

df_duplicates_total.select(sum("count")).display()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) First Approach: Delete duplicated rows

# COMMAND ----------

df_no_duplicates = df.withColumn("date",to_date("transaction_timestamp")).distinct()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) First Approach: Comprobation

# COMMAND ----------

df_duplicates=df_no_duplicates.groupBy("transaction_timestamp","from_address", "to_address","token_id","amount").count().filter("count > 1")
df_duplicates.display()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> `109000 - 108627 = 373 duplicated records` 

# COMMAND ----------

df_no_duplicates.count()

# COMMAND ----------

df_no_duplicates.select("transaction_timestamp","from_address", "to_address","token_id","amount").display()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Second Approach: Duplicated rows based on the key columns: transaction_timestamp, from_address, and to_address
# MAGIC 
# MAGIC In this approach we look for records where, regardless the other columns, the primary key is repeated 
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Warning:** `This method does not look quite accurate.` 

# COMMAND ----------

#Calling the function
df_result = eval(duplicates_func('df','"transaction_timestamp", "from_address", "to_address"'))
#result.display()
print("Number of duplicated rows: "  + str(df_result.count()) )

# COMMAND ----------

df_result.sort(col("duplicated").asc()).display()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> `Note how this two records are not iqual` 

# COMMAND ----------

df.select("transaction_timestamp","from_address", "to_address","token_id","amount").filter(
  (col("transaction_timestamp") == "2022-06-16T18:53:36.000+0000") & (col("from_address") == "0xaf53130f1bd662a7d06eaac7533cfe0fe3262d77") & (col("to_address") == "0x83c8f28c26bf6aaca652df1dbbe0e1b56f8baba2")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Second Approach: Delete duplicated rows

# COMMAND ----------

df_no_duplicates = df.dropDuplicates(["transaction_timestamp","from_address","to_address"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Second Approach: Comprobation

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> `Number of duplicated rows` 

# COMMAND ----------

#Calling the function
df_result = eval(duplicates_func('df_no_duplicates','"transaction_timestamp", "from_address", "to_address"'))
#result.display()
print("Number of duplicated rows: "  + str(df_result.count()) )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> `Select the duplicate records for the same PK used above` 

# COMMAND ----------

df_no_duplicates.select("transaction_timestamp","from_address", "to_address","token_id","amount").filter(
  (col("transaction_timestamp") == "2022-06-16T18:53:36.000+0000") & (col("from_address") == "0xaf53130f1bd662a7d06eaac7533cfe0fe3262d77") & (col("to_address") == "0x83c8f28c26bf6aaca652df1dbbe0e1b56f8baba2")).display()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Conclution: The first approach looks more accurate