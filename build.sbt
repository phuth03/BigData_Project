ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "ProjectSample"
  )
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.4"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.4"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.4.4"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.4.1"
