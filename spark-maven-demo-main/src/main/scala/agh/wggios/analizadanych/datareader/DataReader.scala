package agh.wggios.analizadanych.datareader
import agh.wggios.analizadanych.session._
import agh.wggios.analizadanych.caseclass.FlightCC
import org.apache.spark.sql.{DataFrame, Dataset}

class DataReader(path: String) extends sparksession {
  def read(): Dataset[FlightCC] = {
    import spark.implicits._
    spark.read.format("csv").option("header", value = true).option("inferSchema", value = true).load(path).as[FlightCC]
  }
}
