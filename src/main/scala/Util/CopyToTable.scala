package Util

import java.io.StringReader

import org.apache.spark.rdd.RDD
import org.postgresql.PGConnection
/**
  * Created by 小灰灰 on 2017/9/23.
  */
object CopyToTable extends PsqlConn {
  //COPY FROM将数据从文件复制到表
  //DELIMITER指定分隔文件每行（行）中的列的字符.
  def copyTable(tableName: String, iterable:Iterator[String], columns: List[String]): Unit = {
          connection(a => {
            iterable.foreach(b => {
              val cpManager = a.unwrap(classOf[PGConnection]).getCopyAPI
                .copyIn(
                  s"""
                     |copy $tableName (${columns.mkString(",")})
                     |FROM STDIN WITH NULL 'NULL' DELIMITER '\t'
            """.stripMargin.replaceAll("\n", " "),
                  new StringReader(b))
            })
          })
       }
    }
