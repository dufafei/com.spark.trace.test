package Init

import java.sql.{Connection, DriverManager}

/**
  * Created by 小灰灰 on 2017/9/12.
  */
trait connMysql extends Pattern{
  val url:String
  val user:String
  val pwd:String
  classOf[com.mysql.jdbc.Driver]
  println("加载驱动成功")
   def connection[A](execute:Connection=> A) = {
     val conn=DriverManager.getConnection(url,user,pwd)
     using(conn)(execute)
  }
}
