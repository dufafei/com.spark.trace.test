package Util

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.{BrokerNotAvailableException, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{Json, ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}

import scala.collection.immutable
/**
  *spark streaming使用direct方式读取kafka数据，并存储每个partition读取的offset。
  * 大致思想就是，在初始化 kafka stream 的时候，查看 zookeeper 中是否保存有 offset，
  * 有就从该 offset 进行读取，没有就从最新/旧进行读取。
  * 在消费 kafka 数据的同时，将每个 partition 的 offset 保存到 zookeeper 中进行备份
  * Created by 小灰灰 on 2017/9/16.
  */
class DirectStream() extends  Serializable{

  val logger=Logger.getLogger(DirectStream.this.getClass)
  logger.setLevel(Level.INFO)

  def createDirectStream(ssc: StreamingContext,
                         kafkaParams: Map[String, String],
                         topics: Set[String],
                         batchSave:RDD[String] => Unit
  ):Unit ={
    val groupId=kafkaParams.getOrElse("group.id","test-consumer-group")
    logger.info("Update offsets...")
    //sessionTimeout, connectionTimeout;
    val zkClient = new ZkClient(KafkaProperties.ZookeeperQuorums,5000,5000,ZKStringSerializer) with Serializable
    //topic,partition,offset
    val fromOffset=setOrUpdateOffsets(topics.toSeq, zkClient,groupId)
    logger.info("Start receive data...")
    //参数messageHandler的设置，为了后续处理中能获取到topic，这里形成(topic, message)的tuple：
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    val inputStream=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](
      ssc,
      kafkaParams,
      fromOffset,
      messageHandler)
    var offsetLists = Array[OffsetRange]()
    inputStream.transform (rdd=>{
      offsetLists = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }).map(msg => msg._2).foreachRDD(rdd => {
     /* rdd.foreachPartition(batchSave)*/
      if (!rdd.isEmpty()){
        //处理数据
         logger.info("Start processing data...")
         batchSave(rdd)
        // 提交当前partition的消费信息到zookeeper上面保存
        offsetLists.foreach(x=>{
        val topicDirs = new ZKGroupTopicDirs(groupId,x.topic)
        val zkPath = s"${topicDirs.consumerOffsetDir}/${x.partition}"
        ZkUtils.updatePersistentPath(zkClient,zkPath,x.untilOffset.toString)
        logger.info(s"Offset update: set offset of ${x.topic}/${x.partition} as ${x.untilOffset.toString}")})}
    })

  }
  def setOrUpdateOffsets(topics:Seq[String],
                          zkClient: ZkClient,
                          groupId:String):Map[TopicAndPartition, Long] = {
    val topic2partitions=ZkUtils.getPartitionsForTopics(zkClient,topics)
    //TopicAndPartition
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    topic2partitions.foreach(x=>{
      val topic:String=x._1
      val partitions:Seq[Int]=x._2
      //ZKGroupTopicDirs
      logger.info(s"正在遍历当前分区：$topic")
      val topicDirs = new ZKGroupTopicDirs(groupId, topic)
      logger.info(s"遍历当前${topic}下的分区...")
      partitions.foreach(partition => {
        val zkPath=s"${topicDirs.consumerOffsetDir}/$partition"
        logger.info(
          s"""
             |确保当前分区:${partition}给定的路径在zookeeper中存在，
             |如果不存在创建该path...
           """.stripMargin)
        ZkUtils.makeSurePersistentPathExists(zkClient,zkPath)
        logger.info(
          s"""
             |通过zkClient.readData方法读取Zookeeper中
             |$topic/$partition
             |的Offset ...
           """.stripMargin)
        val utilOffset:String=zkClient.readData[String](zkPath)
        //保存topic,partition到对象TopicAndPartition中
        val tp=TopicAndPartition(topic,partition)
        val kafkaOffset=getMinOffset(zkClient,tp,groupId)
        //获取每个partition的offset
        val offset= try {
          if (utilOffset.equals(null) || utilOffset.equals(""))
            kafkaOffset
          else if(utilOffset.toLong < getMinOffset(zkClient,tp,groupId)){
             logger.info("zookerper信息过期，需要从kafka重新读取...")
             kafkaOffset
          }else {
            utilOffset.toLong
          }
        } catch {
          case e:Exception => getMinOffset(zkClient,tp,groupId)
        }
        //将不同 partition 对应的 offset 增加到 fromOffsets 中
        fromOffsets +=(tp->offset)
        logger.info(s"Offset init: set offset of $topic/$partition as $offset")
      })
    })
        fromOffsets
  }
  /*
   *当第一次启动spark任务或者zookeeper上的数据被删除或设置出错时，
   * 将选取kafak最小的offset开始消费
   *
   * */
  def getMinOffset(zkClient: ZkClient,
                    tp:TopicAndPartition,
                    groupId:String): Long ={
    //time: Long, maxNumOffsets: Int
    //EarliestTime从头取，LatestTime从最后取
    val request=OffsetRequest(immutable.Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime,1)))
    /* 从 kafka 上获取 offset 的时候，需要寻找对应的 leader，
      从 leader 来获取 offset，而不是 broker，不然可能得到的 curOffsets 会是空的（表示获取不到),,
      获取到leader partition 所在的broker id*/
   ZkUtils.getLeaderForPartition(zkClient,tp.topic,tp.partition) match{
     case Some(brokerId) => {
        // BrokerIdsPath = "/brokers/ids"
       ZkUtils.readDataMaybeNull(zkClient,ZkUtils.BrokerIdsPath + "/" + brokerId)._1 match {
         case Some(brokerInfoString) => {
          Json.parseFull(brokerInfoString) match {
            case Some(m) => {
              // 获取该partition所处leader的broker信息
               val  brokerInfo = m.asInstanceOf[Map[String, Any]]
               val  host = brokerInfo("host").asInstanceOf[String]
               val  port = brokerInfo("port").asInstanceOf[Int]
               // 针对所消费的分区，在每个broker建立一个SimpleConsumer对象用来从kafka上获取数据
               //host,port,soTimeout,bufferSize,clientId,
               //client.id：标识发起请求的客户端。默认值为${group.id}。
               val offset = new SimpleConsumer(host, port, 10000, 100000, groupId)
                //getOffsetsBefore的功能是返回某个时间点前的maxOffsetNum个offset
                //这里返回时间点前的一个offset
                .getOffsetsBefore(request)
                .partitionErrorAndOffsets(tp)
                .offsets
                .head
              //构造请求获取consumer端的offset
                offset
            }
            case None => throw new BrokerNotAvailableException("Broker id %d does not exist".format(brokerId))
          }
        }
         case None => throw new BrokerNotAvailableException("Broker id %d does not exist".format(brokerId))
       }
     }
     case None => throw new Exception("No broker for partition %s - %s".format(tp.topic, tp.partition))
   }
  }
}
