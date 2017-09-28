package main.scala.Sources

import Init.SparkInit

/**
  * Created by 小灰灰 on 2017/9/7.
  */
trait dataFromLocal extends SparkInit{
   val path:String
   val datasource=context.textFile(path)
}
