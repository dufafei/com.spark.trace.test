package Handle


class TraceQuery (context:String) extends TraceUrl(context) {

    def TRACKID: Option[String] = OptionTracelog(TraceQuery.ID)
    def getURL:String =getParameter(TraceQuery.URL)


}


object TraceQuery{

  final val ID = "_id"
  final val URL = "url"

  def apply(context: String): TraceQuery = new TraceQuery(context)

}