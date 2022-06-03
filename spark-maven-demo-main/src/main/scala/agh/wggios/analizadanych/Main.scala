package agh.wggios.analizadanych
import agh.wggios.analizadanych.session._
import agh.wggios.analizadanych.datareader.DataReader
import agh.wggios.analizadanych.datawriter.DataWriter
import agh.wggios.analizadanych.transformation.transformation


object Main extends sparksession{

  def main(args: Array[String]): Unit = {
    val df = new DataReader("data/2015-summary.csv").read()
    df.show()
    val filtred_df = df.filter(row => new transformation().USflights(row))
    filtred_df.show()
    new DataWriter("filtred_-summary", df).write()
  }
}
