name := "spark-workshop"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.0"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "1.6.0"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.6"

libraryDependencies += "io.spray" % "spray-can" % "1.3.1"

libraryDependencies += "com.google.guava" % "guava" % "18.0"

libraryDependencies += "org.apache.cassandra" % "cassandra-all" % "2.2.2"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.2.0-rc3"

libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.11" % "1.5.0-M2"

resolvers += Resolver.sonatypeRepo("public")