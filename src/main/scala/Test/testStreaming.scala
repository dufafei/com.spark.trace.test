package Test

import Init.SparkInit
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by 小灰灰 on 2017/9/19.
  */
object testStreaming {
  def main(args: Array[String]): Unit = {

    val init=new {
      val application="streaming"
    }with SparkInit

    val ssc=new StreamingContext(init.context,Seconds(5))

    ssc.checkpoint("D:/")

    /*val  ds=ssc.socketTextStream("192.168.132.105",8888)
    val  result=ds.flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_)
    result.print()*/
    val topics=Set("streaming").map(x => (x,2)).toMap
    val zkQuorum="192.168.132.104:2181,192.168.132.105:2181,192.168.132.106:2181"
    val group="streaming"
    val topicMap=topics.map(x =>(x,1))
    val data=KafkaUtils.createStream(ssc,zkQuorum,group,topics)
    val word=data.map(x=>x._2).flatMap(_.split(" "))
    val updateFunc = (currentValues: Seq[Int], preValue: Option[Int]) => {
      val curr = currentValues.sum
      val pre = preValue.getOrElse(0)
      Some(curr + pre)
    }
    val words=word.map(x=>(x,1)).reduceByKey(_+_).updateStateByKey(updateFunc).print()

    ssc.start()
    //启动程序
    ssc.awaitTermination()
    //阻塞程序

  }

}
