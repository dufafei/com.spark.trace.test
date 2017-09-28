package main.scala.Util

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by 小灰灰 on 2017/9/7.
  */

case class setTime(time:String,add: Int)

trait timeTool extends Serializable{
    val init:setTime

    def addToDay: DateTime ={
      setToday.plusDays(init.add)
    }

    def setToday: DateTime ={
      DateTime.parse(init.time,DateTimeFormat.forPattern("yyyy-MM-dd"))
    }

}
