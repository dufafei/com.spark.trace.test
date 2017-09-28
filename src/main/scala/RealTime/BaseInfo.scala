package RealTime

import Handle.{BaseDataTrace, Tracelog}

/**
  * Created by 小灰灰 on 2017/9/23.
  */
class BaseInfo(traceLog:Tracelog) extends Serializable{
  val bd =BaseDataTrace(traceLog)
  val basePar=BasePar(bd)
}
object BaseInfo{
  def apply(traceLog: Tracelog): BaseInfo = new BaseInfo(traceLog)

}