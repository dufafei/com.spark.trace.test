package Util

import org.apache.hadoop.fs.Path
import org.joda.time.{DateTime, Days}
import org.joda.time.format.DateTimeFormat
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext


 class  LogPath(path: casePath,sc: SparkContext) extends Serializable{

   val logpath: String = getPath.mkString(",")

   def getPath: IndexedSeq[String] = {
   val startTime: DateTime = DateTime.parse(path.startTime,DateTimeFormat.forPattern("yyyyMMdd"))
   val endTime: DateTime = startTime.plusDays(path.interval)
   (0 to Days.daysBetween(startTime, endTime).getDays).map(x => {
    val secondDire: DateTime = startTime.plusDays(x)
     (path.rootDire, secondDire.toString("yyyyMMdd")+".log")
    }).map(b => makePath(b._1,b._2))
       .filter(c => FileSystem.get(sc.hadoopConfiguration).exists(new Path(c)))
   }

   def makePath(rootDire: String, secondDire: String): String = {
   Seq(rootDire,secondDire)
     .mkString("/")
   }

   /*//File.separator是系统默认的文件分割符号，屏蔽了这些系统的区别 == /

   def isExists(rootDire: String,secondDire:String):Boolean ={
   val file = new File(rootDire+File.separator+secondDire)
   if(file.exists() && file.isFile) true
   else false
   }*/

 }



