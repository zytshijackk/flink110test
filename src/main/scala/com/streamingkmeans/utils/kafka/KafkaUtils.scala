package com.streamingkmeans.utils.kafka

import java.util.{HashMap, Map, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

/**
 * @Author: ch
 * @Date: 07/05/2020 12:20 AM
 * @Version 1.0
 * @Describe: 发送数据到kafka对应的主题中
 */
object KafkaUtils {
  val broker_list = "localhost:9092"
  val topic = "test" // kafka topic，Flink 程序中需要和这个统一

  def writeToKafka(point:String,producer: KafkaProducer[String, String]): Unit = {


    val record = new ProducerRecord[String, String](topic, null, null, point)
    producer.send(record)
    System.out.println("发送数据: " + point)
    producer.flush()
  }

  def main(args: Array[String]): Unit = {
    while ( {
      true
    }) {
      val props = new Properties
      props.put("bootstrap.servers", broker_list)
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer") //key 序列化

      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer") //value 序列化

      val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
      Thread.sleep(300)
      val source = Source.fromFile("/Users/zytshijack/Documents/github/git/myrepositories/flink110test/src/main/resources/file/randompoint10MB.txt")
      val lineIterator = source.getLines
      for (line <- lineIterator){
        writeToKafka(line,producer) //将文件的每一行发送到kafka中
      }
    }
  }
}
