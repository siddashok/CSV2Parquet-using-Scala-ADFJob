// Databricks notebook source
// DBTITLE 1,Parameterizing the Asset and yearMonth 
//Uncomment the next two lines for the first time run, for all further run keep them uncommented
//AssetPath = dbutils.widgets.text("AssetPath","<defaultval>")
//yearMonthPath = dbutils.widgets.text("yearMonthPath","<defaultval>")

// COMMAND ----------

// DBTITLE 1,Get Asset and YearMonth detail from <dbutils.widgets.text>
val Asset = dbutils.widgets.get("AssetPath")
val yearMonth = dbutils.widgets.get("yearMonthPath")


// COMMAND ----------

// DBTITLE 1,CSV Files Storage and Container Details 
//provide the Azure storage details where your csv files are located (source)
val CSVstorageAccountName = ""
val CSVstorageAccountAccessKey = ""   
val CSVcontainerName = ""

// COMMAND ----------

// DBTITLE 1,Parquet Files Storage and Container Details 
//provide the Azure storage details where you want to store your parquet files (target)
val ParquetstorageAccountName = ""
val ParquetstorageAccountAccessKey = "" //
val ParquetcontainerName = ""

// COMMAND ----------

// DBTITLE 1,Mount CSV File Container
dbutils.fs.mount(
  source = "wasbs://" + CSVcontainerName + "@" + CSVstorageAccountName + ".blob.core.windows.net",
  mountPoint = "/mnt/" + CSVcontainerName,
  extraConfigs = Map("fs.azure.account.key." + CSVstorageAccountName + ".blob.core.windows.net" -> CSVstorageAccountAccessKey))

// COMMAND ----------

// DBTITLE 1,Read CSV using defined Schema
// Then run this cell to read in the csv using the defined schema, then display the data
// TimeStamp format:  yyy-MM-ddThh:mm:ss.fffZ



import org.apache.spark.sql.types._
sqlContext.setConf("spark.sql.parquet.outputTimestampType","TIMESTAMP_MILLIS")

// Define the source data schema, the below shows each of the valid types for use with TSI
// See the Bulk Upload Private Preview customer guide for more information
// This example requires the CSV columns to match the order of the schema

val schema = StructType(Array(
  <<provide your CSV schema
  forexample:
  StructField("id_string", StringType, true),
  StructField("timestamp", TimestampType, true),
  StructField("series.numericValue_double", DoubleType, true),
  .
  .
  .
  StructField("series.badValue_string", StringType, true)
  >>
))

val data = sqlContext.read
  .format("com.databricks.spark.csv")
  .schema(schema)
  .option("header", "true")
  .load("/mnt/" + CSVcontainerName + "/" + Asset + "/" + yearMonth + "/*.csv")    

display(data).  //display the first 1000 rows 
data.count().    //displays the number of rows 

// COMMAND ----------

// DBTITLE 1,Mount Parquet File Container
dbutils.fs.mount(
  source = "wasbs://" + ParquetcontainerName + "@" + ParquetstorageAccountName + ".blob.core.windows.net",
  mountPoint = "/mnt/" + ParquetcontainerName,
  extraConfigs = Map("fs.azure.account.key." + ParquetstorageAccountName + ".blob.core.windows.net" -> ParquetstorageAccountAccessKey))  

// COMMAND ----------

// DBTITLE 1,CSV to Parquet Conversion
data.write
  .option("compression", "snappy") 
  .mode("overwrite")
  .parquet("/mnt/" + ParquetcontainerName + "/" + Asset + "/" + yearMonth )

// COMMAND ----------

// DBTITLE 1,Read Parquet 
val data2 = sqlContext.read.parquet("/mnt/" + ParquetcontainerName + "/" + Asset + "/" + yearMonth )

display(data2).  //display the first 1000 rows 
data2.count().    //displays the number of rows 

// COMMAND ----------

// DBTITLE 1,Unmount CSV files container
dbutils.fs.unmount("/mnt/"+ CSVcontainerName)

// COMMAND ----------

// DBTITLE 1,Unmount Parquet files Container
dbutils.fs.unmount("/mnt/" + ParquetcontainerName)

// COMMAND ----------

val message = "Parquet conversion for " + Asset + "-" + yearMonth + " completed"
dbutils.notebook.exit(message).  
//you can store this message into a storage file or SQL DB through ADF (Implementation not done as a part of this code)
//this message will be printed once the complete notebook executes successfully
