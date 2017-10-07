package Start

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

object SparkToEs {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setMaster("local[1]")//指定local模式
    conf.setAppName("spark to es")//设置任务名
    conf.set("es.index.auto.create","true")//开启自动创建索引
    conf.set("es.nodes","192.168.132.105,192.168.132.106")//es的节点，多个用逗号分隔
    conf.set("es.port","9200")//端口号
    val sc=new SparkContext(conf)
    val data1 = Map("id" -> 1, "name" -> "tom", "age" -> 19)//第一条数据
    val data2 = Map("id" -> 2, "name" -> "john","age"->25)//第二条数据
    sc.makeRDD(Seq(data1, data2)).saveToEs("spark/es")//添加到索引里面
    println("存储成功！")
    sc.stop()
  }
}
