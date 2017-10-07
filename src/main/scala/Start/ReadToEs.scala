package Start

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
object ReadToEs {

  def main(args: Array[String]): Unit = {

    val conf=new SparkConf()
    conf.setMaster("local[1]")//指定local模式
    conf.setAppName("spark to es")//任务名
    conf.set("es.nodes","192.168.132.105,192.168.132.106")//es节点多个逗号分隔
    conf.set("es.port","9200")
    val sc=new SparkContext(conf)
    val ds=sc.esRDD("spark/es")//读取数据到spark的rdd里面
    print("数量："+ds.count())//统计数量
    sc.stop()
  }
}
