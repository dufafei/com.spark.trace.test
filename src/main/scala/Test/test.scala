package Test

import kafka.common.BrokerNotAvailableException
import kafka.consumer.SimpleConsumer
import kafka.utils.{Json, ZkUtils}

/**
  * Created by 小灰灰 on 2017/9/23.
  */
object test {

  def getSplitKeys(partition:Int): Unit ={
    val s=new Array[Array[Byte]](partition-1)
   for(i <- 1 until partition){
      s(i-1)=Array(i.toByte)
   }
  }

}
