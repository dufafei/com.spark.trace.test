package Util

import org.joda.time.DateTime

/**
  * 大致思想就是，在初始化 kafka stream 的时候，查看 zookeeper 中是否保存有 offset，有就从该 offset 进行读取，没有就从最新/旧进行读取。
  * 在消费 kafka 数据的同时，将每个 partition 的 offset 保存到 zookeeper 中进行备份
  * Created by 小灰灰 on 2017/9/15.
  */
object KafkaProperties {

  final val ZookeeperQuorums = "192.168.132.104:2181,192.168.132.105:2181,192.168.132.106:2181"
  final val KafkaServer="192.168.132.104:9092,192.168.132.105:9092,192.168.132.106:9092"
  final val kafkaParmter= Map[String, String](
    "metadata.broker.list" -> KafkaServer,
    "group.id" -> "streaming"
  )
}
