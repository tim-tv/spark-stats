name := "spark-stats"

version := "1.0"

scalaVersion := "2.11.9"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0",
  "org.apache.spark" %% "spark-mllib" % "2.2.0",
  "org.apache.spark" %% "spark-sql" % "2.2.0",
  "org.apache.commons" % "commons-math3" % "3.3"
)

