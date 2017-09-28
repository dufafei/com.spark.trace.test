package Start
import Util.CopyToTable
import main.scala.Sources.dataFromLocal
import main.scala.Util.{filterTool, setTime}

/**
  * Created by 小灰灰 on 2017/9/7.
  */
object installStart {

  def main(args: Array[String]): Unit = {
    val dataInit=new {
      val application="install"
      val path:String="install-package/*"
    }with dataFromLocal

    val data=dataInit.datasource
    val timeInit= new{
      val init=setTime("2016-04-03",1)
    }with filterTool

     val beginTime=timeInit.setToday
     val endTime=timeInit.addToDay

       lazy val today=data.map(_.split("\t")).filter(x => timeInit.fileterByTime(x,beginTime))
      .groupBy(x=>x(1)).map(x=>x._1->x._2.map(x=>x(2)).toList).collect().toMap
       val addone=data.map(_.split("\t")).filter(x => timeInit.fileterByTime(x,endTime))
      .groupBy(x=>x(1)).map(x=>x._1->x._2.map(x=>x(2)).toList)

      val br=dataInit.context.broadcast(today)
      val analy=addone.map(x=>{
       timeInit.updateInstall(br.value,x)
     }).filter(x => !x._1.equals("-") && !x._2.equals(0)).map(x =>
      List(x._1,x._2).mkString("\t")
      ).repartition(1)
      //631,133
      val table="trace"
      val columns=List("trackid,productid")
      analy.foreachPartition(x => CopyToTable.copyTable(table,x,columns))
  }
}
