package Handle

import java.net.URLDecoder

class TraceUrl(context: String) extends Serializable{

  def getParameter:Map[String,String] = {
    if(context.isEmpty) Map[String,String]()
    else context.split("&")
         .map(_.split("="))
         .filter(_.length==2)
         .map(l=>l.head -> URLDecoder.decode(l.last,"UTF8"))
         .toMap
  }
  def OptionTracelog(key:String):Option[String]={
      getParameter.get(key) match {
        case Some(value) if !value.isEmpty => Some(value)
        case                             _ => None
    }
  }

}