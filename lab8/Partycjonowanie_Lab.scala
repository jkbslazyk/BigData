// Databricks notebook source
// MAGIC %md
// MAGIC ## Jak działa partycjonowanie
// MAGIC 
// MAGIC 1. Rozpocznij z 8 partycjami.
// MAGIC 2. Uruchom kod.
// MAGIC 3. Otwórz **Spark UI**
// MAGIC 4. Sprawdź drugi job (czy są jakieś różnice pomięczy drugim)
// MAGIC 5. Sprawdź **Event Timeline**
// MAGIC 6. Sprawdzaj czas wykonania.
// MAGIC   * Uruchom kilka razy rzeby sprawdzić średni czas wykonania.
// MAGIC 
// MAGIC Powtórz z inną liczbą partycji
// MAGIC * 1 partycja
// MAGIC * 7 partycja
// MAGIC * 9 partycja
// MAGIC * 16 partycja
// MAGIC * 24 partycja
// MAGIC * 96 partycja
// MAGIC * 200 partycja
// MAGIC * 4000 partycja
// MAGIC 
// MAGIC Zastąp `repartition(n)` z `coalesce(n)` używając:
// MAGIC * 6 partycji
// MAGIC * 5 partycji
// MAGIC * 4 partycji
// MAGIC * 3 partycji
// MAGIC * 2 partycji
// MAGIC * 1 partycji
// MAGIC 
// MAGIC ** *Note:* ** *Dane muszą być wystarczająco duże żeby zaobserwować duże różnice z małymi partycjami.*<br/>* To co możesz sprawdzić jak zachowują się małe dane z dużą ilośćia partycji.*

// COMMAND ----------

val slots = sc.defaultParallelism
spark.conf.get("spark.sql.shuffle.partitions")

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.tsv"

val df = spark.read.option("sep", "\t").option("header", "true").csv(parquetDir).repartition(1).groupBy($"requests").sum()

df.explain
df.count

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.tsv"

val df = spark.read.option("sep", "\t").option("header", "true").csv(parquetDir).repartition(7).groupBy($"requests").sum()

df.explain
df.count

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.tsv"

val df = spark.read.option("sep", "\t").option("header", "true").csv(parquetDir).repartition(9).groupBy($"requests").sum()

df.explain
df.count

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.tsv"

val df = spark.read.option("sep", "\t").option("header", "true").csv(parquetDir).repartition(16).groupBy($"requests").sum()

df.explain
df.count

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.tsv"

val df = spark.read.option("sep", "\t").option("header", "true").csv(parquetDir).repartition(24).groupBy($"requests").sum()

df.explain
df.count

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.tsv"

val df = spark.read.option("sep", "\t").option("header", "true").csv(parquetDir).repartition(96).groupBy($"requests").sum()

df.explain
df.count

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.tsv"

val df = spark.read.option("sep", "\t").option("header", "true").csv(parquetDir).repartition(200).groupBy($"requests").sum()

df.explain
df.count

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.tsv"

val df = spark.read.option("sep", "\t").option("header", "true").csv(parquetDir).repartition(2000).groupBy($"requests").sum()

df.explain
df.count

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.tsv"

val df = spark.read.option("sep", "\t").option("header", "true").csv(parquetDir).repartition(4000).groupBy($"requests").sum()

df.explain
df.count

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.tsv"

val df = spark.read.option("sep", "\t").option("header", "true").csv(parquetDir).coalesce(1).groupBy($"requests").sum()

df.explain
df.count

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.tsv"

val df = spark.read.option("sep", "\t").option("header", "true").csv(parquetDir).coalesce(2).groupBy($"requests").sum()

df.explain
df.count

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.tsv"

val df = spark.read.option("sep", "\t").option("header", "true").csv(parquetDir).coalesce(3).groupBy($"requests").sum()

df.explain
df.count

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.tsv"

val df = spark.read.option("sep", "\t").option("header", "true").csv(parquetDir).coalesce(4).groupBy($"requests").sum()

df.explain
df.count

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.tsv"

val df = spark.read.option("sep", "\t").option("header", "true").csv(parquetDir).coalesce(5).groupBy($"requests").sum()

df.explain
df.count

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.tsv"

val df = spark.read.option("sep", "\t").option("header", "true").csv(parquetDir).coalesce(6).groupBy($"requests").sum()

df.explain
df.count

// COMMAND ----------

// MAGIC %md
// MAGIC Dla 4000 partycji czas wykonania to ponad 5 min, dla 2000 partycji o połowę mniej - niceałe 2,5 minuty. Dla 200 partycji czas został zredukowany do 14 sekund, dla 96 już tylko 9 sekund, dla 24 jest to 5 sekund, dla 16 - 3 sekundy, dla 7 ponownie 4 sekundy.
// MAGIC Dla coalesce() z parametrem 1 i 2 czas wykonania to 13 sekund, dla parametru 3 i 4 14 sekund, dla parametrów 5 i 6 jest to 15 sekund.
