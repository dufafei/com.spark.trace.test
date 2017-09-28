package Util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Hdfs {

   def initHdfs:FileSystem ={
     val conf=new Configuration()
     FileSystem.get(conf)
   }

   def readFile(hdfs:FileSystem,sc:SparkContext,path: Path):Option[RDD[String]] ={
    if(hdfs.exists(path)) Some(sc.textFile(path.toString))
    else None
   }

  def readFile(sc:SparkContext,path: Path):Option[RDD[String]] ={
    readFile(initHdfs,sc,path)
  }


}