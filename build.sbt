name := "com.spark.trace.test"

version := "1.0"

scalaVersion := "2.10.4"

// http://mvnrepository.com/artifact/org.apache.spark/spark-core_2.10

libraryDependencies ++= Seq(
   "org.apache.spark" % "spark-core_2.10" % "1.5.2" ,
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
   "org.joda" % "joda-convert" % "1.8.1" % "provided",
   "org.elasticsearch" % "elasticsearch-spark_2.10" % "2.4.4",
   "org.scalatest" % "scalatest_2.10" % "3.0.0-M16-SNAP6",
   "org.mongodb.spark" % "mongo-spark-connector_2.10" % "2.2.0",
   "redis.clients" % "jedis" % "2.6.2"
    ).map( _.exclude("org.mortbay.jetty", "servlet-api").
  exclude("commons-beanutils", "commons-beanutils-core").
  exclude("commons-collections", "commons-collections").
  exclude("commons-collections", "commons-collections").
  exclude("com.esotericsoftware.minlog", "minlog").
  exclude("org.slf4j", "jcl-over-slf4j").
  excludeAll(
     ExclusionRule(organization = "org.eclipse.jetty.orbit")
  ))
    //打包时，排除scala类库
   assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
    //pack插件打包
   packAutoSettings

