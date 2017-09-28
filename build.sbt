name := "com.spark.trace.test"

version := "1.0"

scalaVersion := "2.10.4"

// http://mvnrepository.com/artifact/org.apache.spark/spark-core_2.10

libraryDependencies ++= Seq(
   "org.apache.spark" % "spark-core_2.10" % "1.5.2" % "provided" ,
   "org.apache.spark" % "spark-streaming_2.10" % "1.5.2",
   "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.2" ,
   "org.apache.spark" % "spark-sql_2.10" % "1.5.2" ,
   "org.apache.hbase" % "hbase-client" % "1.0.0",
   "org.apache.hbase" % "hbase-common" % "1.0.0",
   "org.apache.hbase" % "hbase-server" % "1.0.0",
   "org.postgresql"   % "postgresql"    % "9.4-1201-jdbc41",
   "mysql" % "mysql-connector-java" % "5.1.38",
   "org.scalikejdbc" %% "scalikejdbc" % "2.4.1",
   "org.scalikejdbc" %% "scalikejdbc-config" % "2.4.1",
   "joda-time" % "joda-time" % "2.9.3" % "provided",
   "org.joda" % "joda-convert" % "1.8.1" % "provided"
    )map( _.excludeAll(ExclusionRule(organization = "org.mortbay.jetty")) )

packAutoSettings

