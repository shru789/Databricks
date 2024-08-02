# Databricks notebook source
from pyspark.sql.types import *
users_schema=StructType([StructField("Id", IntegerType()),
                         StructField("Name", StringType()),
                         StructField("Gender", StringType()),
                         StructField("Salary", IntegerType()),
                         StructField("Country", StringType()),
                         StructField("Date", StringType())
])

# COMMAND ----------

# MAGIC %sql
# MAGIC use tan.datamaster

# COMMAND ----------

(
spark
.readStream
.schema(users_schema)
.csv("dbfs:/mnt/hexawaredatabricks/raw/stream_in/",header=True)
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/Shruti/stream")
.trigger(once=True)
.table("tan.datamaster.stream")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tan.datamaster.stream

# COMMAND ----------

(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/Shruti/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/Shruti/autoloader")
.trigger(once=True)
.table("tan.datamaster.autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from autoloader;

# COMMAND ----------

(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaEvolutionMode","rescue")
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/Shruti/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/Shruti/autoloader")
.option("mergeSchema",True)
.table("tan.datamaster.autoloader")
)
 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tan.datamaster.autoloader
