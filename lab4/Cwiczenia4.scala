// Databricks notebook source
// MAGIC %md 
// MAGIC Wykożystaj dane z bazy 'bidevtestserver.database.windows.net'
// MAGIC ||
// MAGIC |--|
// MAGIC |SalesLT.Customer|
// MAGIC |SalesLT.ProductModel|
// MAGIC |SalesLT.vProductModelCatalogDescription|
// MAGIC |SalesLT.ProductDescription|
// MAGIC |SalesLT.Product|
// MAGIC |SalesLT.ProductModelProductDescription|
// MAGIC |SalesLT.vProductAndDescription|
// MAGIC |SalesLT.ProductCategory|
// MAGIC |SalesLT.vGetAllCategories|
// MAGIC |SalesLT.Address|
// MAGIC |SalesLT.CustomerAddress|
// MAGIC |SalesLT.SalesOrderDetail|
// MAGIC |SalesLT.SalesOrderHeader|

// COMMAND ----------

//INFORMATION_SCHEMA.TABLES

val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"

val tabela = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM INFORMATION_SCHEMA.TABLES")
  .load()

display(tabela)

// COMMAND ----------

val SalesLT_df = tabela.filter(tabela("TABLE_SCHEMA")==="SalesLT")

display(SalesLT_df)

// COMMAND ----------

val table_list = SalesLT_df.select("TABLE_NAME").as[String]
          .collect.toList


// COMMAND ----------

for(i<- table_list){
  val tabela = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query",s"SELECT * FROM SalesLT.$i")
  .load()
  
  tabela.write.format("delta").mode("overwrite").saveAsTable(i)
  
}

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Pobierz wszystkie tabele z schematu SalesLt i zapisz lokalnie bez modyfikacji w formacie delta

// COMMAND ----------

// MAGIC %md
// MAGIC  Uzycie Nulls, fill, drop, replace, i agg
// MAGIC  * W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
// MAGIC  * Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null
// MAGIC  * Użyj funkcji drop żeby usunąć nulle, 
// MAGIC  * wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
// MAGIC  * Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg() 
// MAGIC    - Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

// COMMAND ----------

//nulle w kolumnach
import org.apache.spark.sql.functions.{col,when, count}
import org.apache.spark.sql.Column

def countCols(columns:Array[String]):Array[Column]={
    columns.map(c=>{
      count(when(col(c).isNull,c)).alias(c)
    })
}

for(i <- table_list.map(x=>x.toLowerCase())){
  val df = spark.read.format("delta").option("header","true").option("inferSchema","true").load(s"dbfs:/user/hive/warehouse/$i")
  df.select(countCols(df.columns):_*).show()
}

// COMMAND ----------

for(i <- table_list.map(x=>x.toLowerCase())){
  val df = spark.read.format("delta").option("header","true").option("inferSchema","true").load(s"dbfs:/user/hive/warehouse/$i")
  val start = df.count()
  val del = df.na.drop("any").count()
  val diff = start-del
  print(i + ": " + diff +"\n")
}

// COMMAND ----------

for( i <- table_list.map(x => x.toLowerCase())){
  val df = spark.read.format("delta").option("header","true").option("inferSchema","true").load(s"dbfs:/user/hive/warehouse/$i")
  val df_new=df.na.fill("0", df.columns)
  df_new.show()
}

// COMMAND ----------

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, DecimalType}
import org.apache.spark.sql.types.DataTypes._

val agreg_df = spark.read.format("delta").option("header","true").option("inferSchema","true").load("dbfs:/user/hive/warehouse/salesorderheader")
val avg1 = agreg_df.select(avg("TaxAmt")).collect()(0)(0)
val avg2 = agreg_df.select(avg("Freight")).collect()(0)(0)
val min1 = agreg_df.select(min("TaxAmt")).collect()(0)(0)
val min2 = agreg_df.select(min("Freight")).collect()(0)(0)
val max1 = agreg_df.select(max("TaxAmt")).collect()(0)(0)
val max2 = agreg_df.select(max("Freight")).collect()(0)(0)

val someData = Seq(Row("MEAN",avg1, avg2),Row("MIN",min1, min2),Row("MAX",max1, max2))

val someSchema = List(
  StructField("Function",StringType, true),
  StructField("TaxAmt", DecimalType(6,2), true),
  StructField("Freight", DecimalType(6,2), true))

val res_df = spark.createDataFrame(spark.sparkContext.parallelize(someData), StructType(someSchema))
display(res_df)

// COMMAND ----------

val df = spark.read.format("delta")
            .option("header","true")
            .option("inferSchema","true")
            .load(s"dbfs:/user/hive/warehouse/product")

display(df)

// COMMAND ----------

display(df.groupBy($"ProductModelId").agg(Map("ListPrice" -> "mean","StandardCost" -> "sum", "ProductID" -> "min")))

// COMMAND ----------

display(df.groupBy($"Color").agg(Map("ListPrice" -> "mean","StandardCost" -> "sum", "ProductID" -> "min")))

// COMMAND ----------

display(df.groupBy($"ProductCategoryID").agg(Map("ListPrice" -> "mean","StandardCost" -> "sum", "ProductID" -> "min")))

// COMMAND ----------

import scala.math._

val floor_round = udf((a: Double) => floor(a))
val diff_catogories = udf((a: Integer, b: Integer) => abs(a-b))
val space_delete = udf((s: String) => s.replace("-", ""))

display(df.select($"ProductCategoryID",floor_round($"StandardCost") as "StandardCostFloorRound", diff_catogories($"ProductCategoryID", $"ProductModelID") as "DifferenCategories", space_delete($"ProductNumber") as "NewName" ))

// COMMAND ----------

val jsonDf = spark.read.option("multiline", "true").json("/FileStore/tables/brzydki.json")

display(jsonDf)
