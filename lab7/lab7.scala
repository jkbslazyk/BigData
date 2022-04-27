// Databricks notebook source
spark.catalog.listDatabases().show()

// COMMAND ----------

spark.sql("create database if not exists Task")
val df_movies = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load("dbfs:/FileStore/tables/lab1/movies.csv")
val df_movies2 = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load("dbfs:/FileStore/tables/lab1/movies.csv")
df_movies.write.mode("overwrite").saveAsTable("Task.movies")
df_movies2.write.mode("overwrite").saveAsTable("Task.movies2")


// COMMAND ----------

spark.catalog.listTables("Task").show()
spark.catalog.tableExists("Task","movies")

// COMMAND ----------

import org.apache.spark.sql.types._

def drop_full(database: String){
  
  val tables = spark.catalog.listTables(s"$database")
  val names=tables.select("name").as[String].collect.toList
  var i = List()
  for( i <- names){
    spark.sql(s"DELETE FROM $database.$i")
  }
}

// COMMAND ----------

drop_full("Task")

// COMMAND ----------


