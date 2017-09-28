package Sources

import Handle.{BaseDataTrace, Tracelog}
import Init.SparkInit
import Util.{LogPath, casePath}
import org.apache.spark.rdd.RDD


trait dataFromTrace  extends SparkInit {


   //Exception in thread "main" java.lang.NullPointerException
   //解决特质的提前构造问题
   //早期成员定义解决的问题是当特质定义抽象成员并且其具体成员依赖于抽象成员时发生的。
   //TODO https://coderbee.net/index.php/scala/20150720/1272

    //this :{ def stop() }  =>
    //TODO http://hongjiang.info/scala-type-system-self-type/
    //scala自身类型
    //自身类型中，可以用this也可以用其他名字，如self。

    val path:casePath

    val data:RDD[String]=context.textFile(new LogPath(path,context).logpath)

    def getTracelog: RDD[Tracelog] = getTracelog(data)

    def getTracelog(rdd: RDD[String]): RDD[Tracelog] = rdd.flatMap(Tracelog(_))

    def toBaseData(line: String): Option[BaseDataTrace] = {
    Tracelog(line) match {
      case Some(t) => Some(BaseDataTrace(t))
      case None    => None
    }
  }

    def toBaseData: RDD[BaseDataTrace] = data.flatMap(toBaseData(_))

}