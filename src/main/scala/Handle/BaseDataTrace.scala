package Handle

import Analyze.UrlMatch

class BaseDataTrace(tracelog: Tracelog) extends Serializable{

   val traceQuery:TraceQuery = tracelog.querystring
   val url:String = traceQuery.getURL
   def severTime:String = tracelog.servertime
   def trackid:Option[String] = traceQuery.TRACKID

  //toStream用法
  //TODO http://blog.csdn.net/cuipengfei1/article/details/40475201

  def getProductID: Option[Int] ={


    UrlMatch.productpageMatch.toStream.map(x => {
        url  match {
        case x(url) => Some(url.toInt)
        case     _  => None
      }
    }).take(1).toList.head
  }

}


object BaseDataTrace{

  def apply(tracelog: Tracelog): BaseDataTrace = new BaseDataTrace(tracelog)

}