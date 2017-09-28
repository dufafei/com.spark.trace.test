package Util

import java.sql.Connection

import Init.Pattern
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings, DB}

/**
  * Created by 小灰灰 on 2017/9/25.
  */
class PsqlConn extends Pattern{
//初始化连接池
  private val url =  "jdbc:postgresql://localhost:5432/mydb"
  private val user = "postgres"
  private val password = "md5a3556571e93b0d20722ba62be61e8c2d"
  classOf[org.postgresql.Driver]
  val settings=ConnectionPoolSettings(
      initialSize = 5,
      maxSize = 20,
      connectionTimeoutMillis = 3000L
    )
  ConnectionPool.singleton(url, user, password, settings)
  def connection[U](execte: Connection => U): U ={
    val conn = ConnectionPool.borrow()
    using(conn)(execte)
  }
}
