package org.example

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.SparkSession

import java.net.URL

object SparkProject {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[4]").appName("Moja-applikacja").getOrCreate()

    import spark.implicits._

    val urlfile = new URL("https://raw.githubusercontent.com/lrjoshi/webpage/master/public/post/c159s.csv")
    val testcsvgit = IOUtils.toString(urlfile, "UTF-8").lines.toList.toDS()
    val testcsv = spark
      .read.option("header", true)
      .option("inferSchema", true)
      .csv(testcsvgit)
    testcsv.show
  }
}
