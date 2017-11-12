package Start

import java.util.Properties

import Util.KafkaProperties
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.Logging

import scala.io.Source
import scala.reflect.io.Path
/**
  * Created by 小灰灰 on 2017/9/22.
  */
object writeKafka {
  def main(args: Array[String]): Unit = {
    val logger=Logger.getLogger(writeKafka.getClass)
    logger.setLevel(Level.INFO)
    val topic="log"
    val props = new Properties()
    props.put("metadata.broker.list", KafkaProperties.KafkaServer)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks","1")
    props.put("producer.type", "async")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    class sendToData() extends Runnable{
      override def run() = {
        val localfile=Source.fromFile("./steaming.text")
        var number=0
        for(line <- localfile.getLines()){
          val message = new KeyedMessage[String, String](topic, line)
          producer.send(message)
          number += 1
          logger.info("line number:"+number)
          Thread.sleep(5000)
        }
         localfile.close()
         producer.close()
      }
    }

    new Thread(new sendToData).start()
    /*错误代码
    val files = Path(".").walkFilter(x => x.isFile && x.name.contains("2017"))
    files.foreach(print)
    val readers = files.map(x => Source.fromString(x.toString()))
    for (reader <- readers) {
        var number=0
        reader.getLines().foreach(line => {
        println(line)
        val message = new KeyedMessage[String, String](topic, line)
        producer.send(message)
        number += 1
        logger.info("line number:"+number)
      })
       producer.close()
    }*/
  }
}
