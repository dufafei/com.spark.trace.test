package Start

import java.util.Properties
import Init.SparkInit
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by 小灰灰 on 2017/9/20.
  */
case class People(name:String,age:Int)

object sparkSql {
  def main(args: Array[String]): Unit = {
    val init= new{
      val application="spark-sql"
    }with SparkInit
     val sqlcontext=new SQLContext(init.context)
    //TODO 第一种
     val data=init.context.textFile("a.txt").map(_.split(" ")).map(x => People(x(0),x(1).toInt))
     import sqlcontext.implicits._
     val tableDF=data.toDF
     println(tableDF.show())
    // val table=tableDF.registerTempTable("a")
//    //TODO  缓存之使用
//     val se=sqlcontext.sql("select * from a")
//     se.registerTempTable("haha")
//     sqlcontext.cacheTable("haha")
//     sqlcontext.uncacheTable("haha")
//    //TODO 第二种
//     val  schemaString = "name,age"
//     val  schema = StructType(schemaString.split(",").map(filedName =>
//                               StructField(filedName,StringType)))
//     //StructType(Array(StructField("name",StringType), StructField("age",IntegerType,true),
//     // ))
//     val data1=init.context.textFile("./").map(_.split(" ")).map(x => Row(x(0),x(1)))
//     val dataframe=sqlcontext.createDataFrame(data1,schema)
//     dataframe.registerTempTable("pp")
//     sqlcontext.sql("select * from pp").show()
//     //TODO 注册UDF
//     sqlcontext.udf.register("sums",(str1:String,str2:String) => str1 + str2)
//     //TODO 使用UDF
//     sqlcontext.sql("select sums(name,age) from pp")
//     //TODO UDAF
//
//     //TODO 保存DF到hive分区表
//
//    //TODO   整合postgresql，读取
//    val postgresql:DataFrame=sqlcontext.read.format("jdbc").options(Map(
//      "url"->"jdbc:postgresql://localhost:5432/mydb",
//      "dbtable"->"man",
//      "user"->"postgres",
//      "password"->"md5a3556571e93b0d20722ba62be61e8c2d"
//    )).load()
//    postgresql.show()
    //TODO 写入
    val properties = new Properties()
    properties.setProperty("user","postgres")
    properties.setProperty("password","md5a3556571e93b0d20722ba62be61e8c2d")
    tableDF.write.mode(SaveMode.Append).jdbc("jdbc:postgresql://localhost:5432/mydb","sparksql",properties)
    //TODO 保存DF到本地

  }
}
