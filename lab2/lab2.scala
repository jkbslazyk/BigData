// Databricks notebook source
// MAGIC %md 
// MAGIC ## ZAD1
// MAGIC 
// MAGIC Wybierz jeden z plików csv z poprzednich ćwiczeń (w tym przypadku będzie to actors.csv) i stwórz ręcznie schemat danych. Stwórz DataFrame wczytując plik z użyciem schematu. 

// COMMAND ----------

import org.apache.spark.sql.types.{IntegerType, StringType, StructType, StructField}

val schemat = StructType(Array(
  StructField("imdb_title_id", StringType, false),
  StructField("ordering", IntegerType, false),
  StructField("imdb_name", StringType, false),
  StructField("category", StringType, false),
  StructField("job", StringType, true),
  StructField("characters", StringType, false)))


// COMMAND ----------

val ActorsWithSchema = spark.read.format("csv")
.option("header","true")
.schema(schemat)
.load("dbfs:/FileStore/tables/lab1/actors.csv")

display(ActorsWithSchema)

// COMMAND ----------

// MAGIC %md 
// MAGIC ## ZAD2
// MAGIC 
// MAGIC Użyj kilku rzędów danych z jednego z plików csv i stwórz plik json. Stwórz schemat danych do tego pliku (wykorzystam plik i schemat stworzony wyżej). Przydatny tool to sprawdzenie formatu danych. https://jsonformatter.curiousconcept.com/ 

// COMMAND ----------

val aktorzy_json = spark.read.format("json")
      .option("multiline","true")
      .schema(schemat)
      .load("dbfs:/FileStore/tables/aktorzy.json")

display(aktorzy_json)


// COMMAND ----------

// MAGIC %md 
// MAGIC ## ZAD4
// MAGIC 
// MAGIC Użycie Read Modes.  
// MAGIC 
// MAGIC Wykorzystaj posiadane pliki bądź dodaj nowe i użyj wszystkich typów oraz ‘badRecordsPath’, zapisz co się dzieje. Jeśli jedna z opcji nie da żadnych efektów, trzeba popsuć dane. 

// COMMAND ----------

import scala.collection.mutable.ListBuffer

val schemat = StructType(Array(
  StructField("subject", StringType, false),
  StructField("mark", IntegerType, false)))

val row1 = "{'subject': 'maths', 'mark':4}"
val row2 = "{'subject':'german', 'mark':3}"
val row3 = "{'subject':'english'}"
val row4 = "{zzz}"

Seq(row1, row2, row3, row4).toDF().write.mode("overwrite").text("/FileStore/tables/marks.json")


// COMMAND ----------

val Permissive = spark.read.format("json")
  .schema(schemat)
  .option("mode", "PERMISSIVE")
  .load("/FileStore/tables/marks.json")

display(Permissive)

// COMMAND ----------

val DropMalFormed = spark.read.format("json")
  .schema(schemat)
  .option("mode", "DROPMALFORMED")
  .load("/FileStore/tables/marks.json")

display(DropMalFormed)

// COMMAND ----------

val FailFast = spark.read.format("json")
  .schema(schemat)
  .option("mode", "FAILFAST")
  .load("/FileStore/tables/marks.json")

display(FailFast)

// COMMAND ----------

val BadRecord = spark.read.format("json")
  .schema(schemat)
  .option("badRecordsPath", "/FileStore/tables/incorrect")
  .load("/FileStore/tables/marks.json")

display(BadRecord)

// COMMAND ----------

val x = spark.read.format("json")
      .option("multiline","true")
      .schema(schemat)
      .load("dbfs:/FileStore/tables/marks.json")

display(x)

// COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/marks.json", true)

// COMMAND ----------

// MAGIC %md 
// MAGIC ## ZAD5
// MAGIC 
// MAGIC Użycie DataFrameWriter. 
// MAGIC 
// MAGIC Zapisz jeden z wybranych plików do formatów (‘.parquet’, ‘.json’). Sprawdź, czy dane są zapisane poprawnie, użyj do tego DataFrameReader. Opisz co widzisz w docelowej ścieżce i otwórz używając DataFramereader. 

// COMMAND ----------

Permissive.write.format("parquet").mode("overwrite").save("/FileStore/tables/grades.parquet")
val marks = spark.read.format("parquet").load("/FileStore/tables/grades.parquet")
display(marks)

// COMMAND ----------

Permissive.write.format("json").mode("overwrite").save("/FileStore/tables/grades.json")
val marks = spark.read.format("json").load("/FileStore/tables/grades.json")
display(marks)


// COMMAND ----------


