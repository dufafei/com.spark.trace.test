package Util

import java.sql.PreparedStatement
import Init.connMysql

/**
  * Created by 小灰灰 on 2017/9/12.
  */
object ToMysql extends PsqlConn{
     val sema=new{
       val url:String= "jdbc:mysql://192.168.132.104:3306/mysql?character=utf8"
       val user:String="java"
       val pwd:String="123456"
      }with connMysql
  def myfun(iterator: Iterator[(String,String)]): Unit ={
      val sql="insert into test values (?,?)"
      var ps:PreparedStatement=null
      try {
        sema.connection(x => {
           ps = x.prepareStatement(sql)
           iterator.foreach(a => {
             ps.setString(1, a._1)
             ps.setString(2, a._2)
             ps.executeUpdate()
             if(ps != null) ps.close()
          })
        })
      }
      catch {
        case e:Exception => println(e)
      }
  }
}
