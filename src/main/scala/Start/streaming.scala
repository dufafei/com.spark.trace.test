package Start

import Handle.Tracelog
import Init.SparkInit
import RealTime.BaseInfo
import Util.{DirectStream, KafkaProperties}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by 小灰灰 on 2017/9/14.
  * sparkstreaming Direct模板
  */
object streaming {

  def main(args: Array[String]): Unit = {
    val init = new {
      val application = "streaming"
    } with SparkInit
    val ssc = new StreamingContext(init.context, Seconds(5))

    val topic = Set("log")

    def batchSave(rdd: RDD[String]):Unit = {
      rdd.foreach(println)
     /* rdd.map(x => {
        val tracelog=Tracelog(x).get
        BaseInfo(tracelog)
      }).map(x => x.basePar)
        .map(x =>(x,1))
        .filter(x => !x._1.trackid.equals("-") && !x._1.productID.equals(0))
        .reduceByKey(_+_)
        .map(x => {
            (x._1.trackid,x._1.productID,x._1.servertime,x._2).toString()
          })
        .collect().foreach(println)*/
    }
    new DirectStream().createDirectStream(ssc, KafkaProperties.kafkaParmter, topic, batchSave)
    ssc.start()
    //启动程序
    ssc.awaitTermination()
    //阻塞程序
  }
}

