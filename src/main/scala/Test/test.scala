package Test

import Init.SparkInit
import Util.PsqlConn
import kafka.common.BrokerNotAvailableException
import kafka.consumer.SimpleConsumer
import kafka.utils.{Json, ZkUtils}

/**
  * Created by 小灰灰 on 2017/9/23.
  */
object test {
  def main(args: Array[String]): Unit = {
    val init=new{
     val application: String = "test"
    }with SparkInit
    init.context.textFile("steaming.txt").foreach(println)
  }
}
