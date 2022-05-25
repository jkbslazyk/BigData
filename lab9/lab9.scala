// Databricks notebook source
val df_nested = spark.read.format("json").option("multiLine", "true").load("/FileStore/tables/Nested.json")

// COMMAND ----------

df_nested.show

// COMMAND ----------

df_nested.printSchema()

// COMMAND ----------

val df_nested2 = nested_json.withColumn("newCol", $"pathLinkInfo".dropFields("alternateName", "captureSpecification", "cycleFacility", "endGradeSeparation","endNode"))


df_nested2.printSchema()

// COMMAND ----------

val list = List(1, 2, 3 ,4)
val result = list.foldLeft(0)(_ + _)
println(result)


// COMMAND ----------

class Foo(val name: String, val age: Int, val sex: Symbol)

object Foo {
  def apply(name: String, age: Int, sex: Symbol) = new Foo(name, age, sex)
}

val fooList = Foo("Hugh Jass", 25, 'male) ::
              Foo("Biggus Dickus", 43, 'male) ::
              Foo("Incontinentia Buttocks", 37, 'female) ::
              Nil

val stringList = fooList.foldLeft(List[String]()) { (z, f) =>
  val title = f.sex match {
    case 'male => "Mr."
    case 'female => "Ms."
  }
  z :+ s"$title ${f.name}, ${f.age}"
}

// COMMAND ----------

stringList(0)

// COMMAND ----------

stringList(1)

// COMMAND ----------

stringList(2)
