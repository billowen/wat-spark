name := "spark-job"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.1"  % "provided"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.6"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"

test in assembly := {}
assemblyJarName in assembly := "spark-job.jar"