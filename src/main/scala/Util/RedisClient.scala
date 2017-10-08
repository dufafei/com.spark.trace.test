package Util

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * Created by 小灰灰 on 2017/9/24.
  */
object RedisClient {

  def main(args: Array[String]): Unit = {

   val config= new JedisPoolConfig
    config.setMaxTotal(3)
    config.setMaxIdle(10)
   val host= "192.168.132.106"
   val port= 6379
   val jedispool= new JedisPool(config,host,port)
   val client=jedispool.getResource
   println(client.ping())
    client.set("name","dufafei")
    println(client.get("name"))
    // 业务操作完成，将连接返回给连接池
   if(client.isConnected)
     jedispool.returnResource(client)
    // 应用关闭时，释放连接池资源
     jedispool.destroy()
  }
}
