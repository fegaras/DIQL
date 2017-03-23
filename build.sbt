val spark = "org.apache.spark" % "spark-core_2.11" % "2.1.0"

lazy val commonSettings = Seq(
  organization := "uta.edu",
  version := "0.1",
  scalaVersion := "2.11.6"
)

lazy val root = (project in file("."))
  .settings(
    commonSettings,
    name := "DIQL",
    libraryDependencies += spark
  )
