package main.scala.Util

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by 小灰灰 on 2017/9/7.
  */
trait filterTool extends timeTool{

  def fileterByTime(array: Array[String], time: DateTime): Boolean = {
    val arrayTime = DateTime.parse(array(0), DateTimeFormat.forPattern("yyyy-MM-dd"))
    arrayTime.isEqual(time)
  }

  def updateInstall(map1: Map[String,List[String]], map2:(String,List[String])):(String,Int) = {
    //更新第一天用户新安装的包
      map1.get(map2._1) match {
        case Some(value) => map2._1 -> map2._2.diff(value).size
        case None        => "-"  -> 0
      }
   }
}
