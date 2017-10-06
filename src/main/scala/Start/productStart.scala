package Start

import Handle.BaseDataTrace
import Sources.dataFromTrace
import Util.{ToMysql, casePath}
import org.apache.spark.rdd.RDD

object productStart {
  def main(args: Array[String]): Unit = {

       val init = new {
       val   application:String="productInfo"
       val   path:casePath=casePath(startTime = "20170410")
       }with dataFromTrace

    /*在Driver端的程序中创建了一个对象，而在各个Executor中会用到这个对象
    由于Driver端代码与Executor端的代码运行在不同的JVM中，甚至在不同的节点上
    因此必然要有相应的序列化机制来支撑数据实例在不同的JVM或者节点之间的传输。*/

    val datasource:RDD[BaseDataTrace]=init.toBaseData


     //task不能序列化的原因分析
    //TODO  http://blog.csdn.net/javastart/article/details/51206715

       val productInfo=
       datasource
        .map(x => {
          val visiterID = x.trackid.getOrElse("-")
          val productID = x.getProductID match {
                          case Some(value) => value
                          case _ => 0
          }
          (visiterID, productID)
        }).filter(a => !a._1.equals("-") && !a._2.equals(0))
        .groupBy(x => x._1)
           .map(b => b._1 -> b._2.map(c => c._2).toList.mkString(","))
              .sortBy(d => d._2.length)
         //true升序，false降序，第三个参数代表排序后分区的个数

        //val result=productInfo.saveAsTextFile("/home/hadoop/test/result")
        productInfo.foreachPartition(data => ToMysql.myfun(data))
        init.context.stop()

     /*val productInfo1=data.map(x => {
           val trackid=x.trackid.getOrElse("-")
           val productid=x.getProductID match{
             case Some(value) => value
             case _           => 0
           }
        (trackid,productid)
      }).filter(a => !a._1.equals("-") && !a._2.equals(0))
         .sortBy(b => b._1)
         .repartition(1)
    //1975,734

     val result1=productInfo1.saveAsTextFile("./result1")*/

    }
}