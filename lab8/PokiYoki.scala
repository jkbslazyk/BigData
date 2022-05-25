// Databricks notebook source
def intSliceing(s: List[Integer], x: Integer, y: Integer) = {
  val z = s.slice(x,y)
  if(z.isEmpty){
    "List is empty"
  }
  else{
    z
  }
}

// COMMAND ----------

intSliceing(List(1,2,3,4,5,6),2,4)

// COMMAND ----------

intSliceing(List(1,2,3,4,5,6),7,9)

// COMMAND ----------

import scala.util.matching.Regex

def islowercase(str: String)  = {
  val regex = "^[a-z]+$"
  str.matches(regex)
}

// COMMAND ----------

islowercase("asdasfdsf")

// COMMAND ----------

islowercase("ASD")

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

def intAverage(col: String, DF: DataFrame) = {
  if( DF.columns.contains(col)){
    if(DF.schema(col).dataType.typeName == "integer"){
          DF.select(avg(col)).show()
    }
    else
      printf("Column type is not Integer")
  }
  else
    printf("There is no selected column")
}

// COMMAND ----------

import spark.implicits._
val data = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
val rdd = spark.sparkContext.parallelize(data)
val dfFromRDD1 = rdd.toDF()
val df2 = dfFromRDD1.withColumnRenamed("_1","str")
           .withColumnRenamed("_2","int")

// COMMAND ----------

df2.show

// COMMAND ----------

intAverage("int",df2)

// COMMAND ----------

intAverage("ints",df2)

// COMMAND ----------

intAverage("str",df2)

// COMMAND ----------

def checkIfNotOutOfRange(s: List[Integer], x: Integer) = {
  if(s.size<=x){
    printf("Out of Range")
  }
  else{
    s(x)
  }
}

// COMMAND ----------

checkIfNotOutOfRange(List(1,2,3,4,5),7)

// COMMAND ----------

checkIfNotOutOfRange(List(1,2,3,4,5),3)

// COMMAND ----------

def ColMin(col: String, DF: DataFrame) = {
  if( DF.columns.contains(col)){
    if(DF.schema(col).dataType.typeName == "integer" | DF.schema(col).dataType.typeName == "double" | DF.schema(col).dataType.typeName == "float"){
          DF.select(min(col)).show()
    }
    else
      printf("Column type is not Numerical")
  }
  else
    printf("There is no selected column")
}

// COMMAND ----------

ColMin("int",df2)

// COMMAND ----------

ColMin("str",df2)

// COMMAND ----------

def ColMax(col: String, DF: DataFrame) = {
  if( DF.columns.contains(col)){
    if(DF.schema(col).dataType.typeName == "integer" | DF.schema(col).dataType.typeName == "double" | DF.schema(col).dataType.typeName == "float"){
          DF.select(max(col)).show()
    }
    else
      printf("Column type is not Numerical")
  }
  else
    printf("There is no selected column")
}

// COMMAND ----------

ColMax("int",df2)

// COMMAND ----------

ColMax("str",df2)
