package RealTime

import Handle.BaseDataTrace

/**
  * Created by 小灰灰 on 2017/9/23.
  */

                /*vid:String, 访问者id
                 siteId:Int,  //站点id
                 serverTime:Long, //记录的系统时间
                 host:String,
                 remoteHostIp:String,
                 flatForm:Int,    //平台
                 language:Int,
                 url:String, //地址
                 urlRef:Option[String],
                 productId:Option[Int],
                 orderid:Option[Int], //订单id
                 userid:Int*/
case class BasePar(
                    trackid:String,
                    productID:Int,
                    servertime:String)
object BasePar {
  def apply(bd:BaseDataTrace): BasePar =BasePar(
    bd.trackid.getOrElse("-"),
    bd.getProductID.getOrElse(0),
    bd.severTime)

  /*bd.idVisitor.getOrElse("-"),
    bd.siteId,
    bd.serverTime,
    bd.getHost,
    bd.getHostId,
    bd.flatFrom,
    bd.lanuage,
    bd.url,
    bd.urlRef,
    bd.getProductId,
    bd.orderSucceed,
    bd.getUid.getOrElse(0))*/
}
