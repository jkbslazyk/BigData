ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.3",
  "org.apache.spark" %% "spark-sql" % "3.1.3",
  "io.lemonlabs" % "scala-uri_sjs1_2.12" % "4.0.2"
)

lazy val root = (project in file("."))
  .settings(
    name := "tx"
  )

