ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.12.20"

lazy val root = (project in file("."))
  .settings(
    name := "iomete-udfs",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.7" % Provided,
      "org.apache.hive" % "hive-exec" % "2.3.10" % Provided,
      "org.scalatest" %% "scalatest" % "3.2.18" % Test
    )
  )
