package Start

import Init.SparkInit
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by 小灰灰 on 2017/9/20.
  */
case class People(name:String,age:String)
object sparkSql {
  def main(args: Array[String]): Unit = {
    val init= new{
      val application="spark-sql"
    }with SparkInit
     val sqlcontext=new SQLContext(init.context)
     //TODO 第一种
     val data=init.context.textFile(".").map(_.split(" ")).map(x => People(x(0),x(1)))
     import sqlcontext.implicits._
     val table=data.toDF.registerTempTable("a")
     val se=sqlcontext.sql("select * from a").show()
    //TODO 第二种
     val  schemaString = "name,age"
     val  schema = StructType(schemaString.split(",").map(filedName =>
                               StructField(filedName,StringType)))
     //StructType(Array(StructField("name",StringType), StructField("age",IntegerType,true),
     // ))
     val data1=init.context.textFile("./").map(_.split(" ")).map(x => Row(x(0),x(1)))
     val dataframe=sqlcontext.createDataFrame(data1,schema)
     dataframe.registerTempTable("pp")
     sqlcontext.sql("select * from pp").show()
     //TODO 注册UDF
     sqlcontext.udf.register("sums",(str1:String,str2:String) => str1 + str2)
     //TODO 使用UDF
     //TODO 保存DF到本地
     //TODO 保存DF到hive分区表
  }
}
