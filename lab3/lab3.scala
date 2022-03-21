// Databricks notebook source
// MAGIC %md Names.csv 
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
// MAGIC * Odpowiedz na pytanie jakie jest najpopularniesze imię?
// MAGIC * Dodaj kolumnę i policz wiek aktorów 
// MAGIC * Usuń kolumny (bio, death_details)
// MAGIC * Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
// MAGIC * Posortuj dataframe po imieniu rosnąco

// COMMAND ----------

import org.apache.spark.sql.functions._

val startTime = System.currentTimeMillis()
val filePath = "dbfs:/FileStore/tables/lab1/names.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
val resultDf = namesDf.select("*").withColumn("height(Feet)", col("height")*25/64)
              .withColumn("date_of_birth", when(to_date(col("date_of_birth"),"yyyy-MM-dd").isNotNull, to_date(col("date_of_birth"),"yyyy-MM-dd"))
              .when(to_date(col("date_of_birth"),"yyyy MM dd").isNotNull, to_date(col("date_of_birth"),"yyyy MM dd"))
              .when(to_date(col("date_of_birth"),"dd.MM.yyyy").isNotNull, to_date(col("date_of_birth"),"dd.MM.yyyy"))
              .when(to_date(col("date_of_birth"),"yyyy MMMM dd").isNotNull, to_date(col("date_of_birth"),"yyyy MMMM dd"))
              .as("FormatedBirthDate"))
              .withColumn("date_of_death", when(to_date(col("date_of_death"),"yyyy-MM-dd").isNotNull, to_date(col("date_of_death"),"yyyy-MM-dd"))
              .when(to_date(col("date_of_death"),"yyyy MM dd").isNotNull, to_date(col("date_of_death"),"yyyy MM dd"))
              .when(to_date(col("date_of_death"),"dd.MM.yyyy").isNotNull, to_date(col("date_of_death"),"dd.MM.yyyy"))
              .when(to_date(col("date_of_death"),"yyyy MMMM dd").isNotNull, to_date(col("date_of_death"),"yyyy MMMM dd"))
              .as("FormatedDeathDate"))
              .withColumn("yearsDiff",round(months_between( when(col("date_of_death").isNull,current_date()).otherwise(col("date_of_death")),col("date_of_birth"),true).divide(12),0))
              .drop("bio")
              .drop("death_details")
              
val endTime = System.currentTimeMillis()
val timeTakenDf = resultDf.withColumn("time_taken", lit((endTime-startTime)/1e3d)).explain(true)

display(timeTakenDf)

// COMMAND ----------

val tempDf = resultDf.select(split(col("name"), " ").as("NameArray"))
            .withColumn("only_name", slice(col("NameArray"),1,1)).drop("NameArray")
            .groupBy("only_name").count().orderBy(desc("count"))

display(tempDf)
//najpopularniejszym imieniem jest John

// COMMAND ----------

resultDf.select(split(col("name"), " ").as("NameArray"))
            .withColumn("only_name", slice(col("NameArray"),1,1)).drop("NameArray")
            .groupBy("only_name").count().orderBy(desc("count")).explain(true)

// COMMAND ----------

val new_column_names=diffDf.columns.map(c=>c.replace("_"," ").split(' ').map(_.capitalize).mkString(" ").replace(" ","") )
val correctColumnDf = diffDf.toDF(new_column_names:_*).orderBy(asc("Name"))
display(correctColumnDf)

// COMMAND ----------

// MAGIC %md Movies.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
// MAGIC * Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
// MAGIC * Usuń wiersze z dataframe gdzie wartości są null

// COMMAND ----------

val startTime = System.currentTimeMillis()

val filePath = "dbfs:/FileStore/tables/lab1/movies.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

val resultDf = namesDf.withColumn("published_ago",round(months_between(date_format(current_timestamp(),"yyyy"),col("year"),true).divide(12),0))
              .withColumn("corrected_budget",regexp_replace($"budget", "[^0-9]", "")).na.drop("any")

val endTime = System.currentTimeMillis()
val timeTakenDf = resultDf.withColumn("time_taken", lit((endTime-startTime)/1e3d))

display(timeTakenDf)

// COMMAND ----------

// MAGIC %md ratings.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dla każdego z poniższych wyliczeń nie bierz pod uwagę `nulls` 
// MAGIC * Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
// MAGIC * Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
// MAGIC * Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
// MAGIC * Dla jednej z kolumn zmień typ danych do `long` 

// COMMAND ----------

import org.apache.spark.sql._

val startTime = System.currentTimeMillis()

val filePath = "dbfs:/FileStore/tables/lab1/ratings.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

def columnAverage(colList: Array[String]): Column = {
  var denominator = lit(0)
  var numerator = lit(0)
  var value = lit(0)
  var reverseList = colList.reverse
  reverseList.foreach{colName =>
    value = value + 1
    denominator = denominator + when(col(colName).isNull,lit(0)).otherwise(col(colName))
    numerator = numerator + value * when(col(colName).isNull,lit(0)).otherwise(col(colName))
  }
  when(denominator === lit(0),lit(null)).otherwise(numerator/denominator)
}

val columns = namesDf.columns.filter(c => c.contains("votes_"))

val resultDf = namesDf.withColumn("AVG",columnAverage(columns))
            .withColumn("difference",abs($"weighted_average_vote" - abs($"AVG")))

val endTime = System.currentTimeMillis()
val timeTakenDf = resultDf.withColumn("time_taken", lit((endTime-startTime)/1e3d))

display(timeTakenDf)

// COMMAND ----------

val ratingsDf = timeTakenDf.select("females_allages_avg_vote","males_allages_avg_vote").agg(avg("females_allages_avg_vote"), avg("males_allages_avg_vote"))
display(ratingsDf) // oceny kobiet sa nieco lepsze

// COMMAND ----------

import org.apache.spark.sql.types._

val LongDf = ratingsDf.withColumn("mean_vote",col("avg(females_allages_avg_vote)").cast(LongType))
display(LongDf)

// COMMAND ----------

// MAGIC %md ZAD2
// MAGIC 
// MAGIC Przejdź przez Spark UI i opisz w kilku zdaniach co można znaleźć w każdym z elementów Spark UI.

// COMMAND ----------

/*
Jobs - pokazuje statusy wszystkich zadań w aplikacji (active, complited, failed)
Stages - pokazuje obecnyny stan wszystkich etapów dla zadań w aplikacji
Storage - pokazuje informacje o zagospodarowaniu pamięcią, stworzone partycje oraz ich rozmiar 
Environment - pokazuje informacje o zmiennych środowiskowych i konfiguracyjnych a także właściwości syatemowe
Executors - pokazuje informacje na temat executorów stworzonych dla aplikacji takie jak wykorzystywana pamięc i dysk czy zadania
SQL - pokazuje tabele zawierającą listę zapytań SQL oraz bardziej szczegółowe informacje na ich temat
*/

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ZAD4

// COMMAND ----------

val jdbcDF = spark.read
  .format("jdbc")
  .option("url", "jdbc:sqlserver://bidevtestserver.database.windows.net:1433;database=testdb")
  .option("dbtable", "(SELECT * from INFORMATION_SCHEMA.TABLES) emp_alias")
  .option("user", "sqladmin")
  .option("password", "$3bFHs56&o123$")
  .load()

// COMMAND ----------

display(jdbcDF)
