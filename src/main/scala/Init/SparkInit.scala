package Init


import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

trait SparkInit extends Serializable{

//     protected val minExecutors:String ="1"
//
//     protected val maxExecutors:String ="80"
//
//     protected val initialExecutors:String ="20"
//
//     protected val confMap = Map(
//          "spark.dynamicAllocation.enabled" ->"true",
//          "spark.dynamicAllocation.minExecutors" ->minExecutors,
//          "spark.dynamicAllocation.maxExecutors" ->maxExecutors,
//          "spark.dynamicAllocation.initialExecutors" ->initialExecutors
//     )
     val application:String

     @transient
     val conf=new SparkConf()
     .setAppName(application)
     .setMaster("local[2]")
   /*  def getConf:SparkConf= {
          confMap.map(x=>conf.set(x._1,x._2))
          conf
     }*/
     @transient
     val context=new SparkContext(conf)
}