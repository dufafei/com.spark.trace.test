package Init


import org.apache.spark.{SparkConf, SparkContext}

trait SparkInit extends Serializable{

     val application:String

     @transient
     val conf=new SparkConf()
     .setAppName(application)
     .setMaster("local[2]")

     @transient
     val context=new SparkContext(conf)
}