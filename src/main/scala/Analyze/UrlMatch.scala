package Analyze

import scala.util.matching.Regex

object UrlMatch {
   //产品页
   lazy val productpageMatch:List[Regex] = List(
     "https?://[mwshop]+\\..*/product/.*-(\\d+)\\.html.*".r,
     "https?://[mwshop]+\\..*/product-(\\d+)\\.html.*".r,
     "https?://[mwshop]+\\..*/product/(\\d+)\\.html.*".r)
}