package Util

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}
import org.apache.spark.Logging

/**
  * Created by 小灰灰 on 2017/9/23.
  */
object HbaseConn{
  /*
  * 当业务需要大量去连接redis或者hbase的时候，
  * 大量的连接会造成socket的大量占用，
  * 导致的结果就是服务器没有更多的端口去分配，
  * 这种情况下的最好解决方案就是实现客户端连接的单例模式，
  * Connection是线程安全的,推荐使用单例
  * 保持连接永远是同一个。
  */
  val server="192.168.132.104,192.168.132.105,192.168.132.106"
  val port="2181"
  def getConnection():Connection ={
    val conf = HBaseConfiguration.create()
    conf.addResource("hbase-site.xml")
    conf.set("hbase.zookeeper.quorum",  server)
    conf.set("hbase.zookeeper.property.clientPort", port)
    ConnectionFactory.createConnection(conf)
  }
  def getAdmin():Admin ={
    getConnection().getAdmin
  }
}
