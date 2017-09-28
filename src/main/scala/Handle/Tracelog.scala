package Handle


class Tracelog(time:String,context:String) extends TraceUrl(context) {

 lazy val servertime: String = getParameter(Tracelog.SERVER_TIME)
//apply() 返回指定键的值，如果不存在返回 Map 的默认方法
 lazy val querystring: TraceQuery = TraceQuery(getParameter(Tracelog.QUERY_STRING))

}

object Tracelog{
  final val QUERY_STRING="QUERY_STRING"
  final val SERVER_TIME="SERVER_TIME"

  def apply(line:String):Option[Tracelog]={
    if(line.isEmpty) None
    else line.split(",")  match {
      case Array(time,context) => Some(new Tracelog(time,context))
      case                   _ => None
    }
  }
}