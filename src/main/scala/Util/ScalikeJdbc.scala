package Util



import java.io.{ByteArrayInputStream, StringReader}
import java.sql.PreparedStatement

import Init.connMysql
import main.scala.Sources.dataFromLocal
import scalikejdbc.DB
/**
  * Created by 小灰灰 on 2017/9/25.
  */
object ScalikeJdbc extends PsqlConn{

    /**
      *使用scalikejdbc访问mysql
      * */

     val dataInit=new {
      val application="install"
      val path:String="install-package/*"
     }with dataFromLocal

  def myfun(iterator: Iterator[(String,String)]): Unit ={
    import scalikejdbc._
    iterator.foreach(x => {
      //绑定变量的个数
      val bytes = Array[Byte](1 ,2)
      val in = new ByteArrayInputStream (bytes)
      val bytesBinder = ParameterBinder(
        value = in,
        (stmt: PreparedStatement, idx: Int) => stmt.setBinaryStream(idx, in, bytes.length)
      )
      sql"insert into test (time,user,install) values ($bytesBinder)".update.apply()
    })
  }
  def main(args: Array[String]): Unit = {



  }

}
