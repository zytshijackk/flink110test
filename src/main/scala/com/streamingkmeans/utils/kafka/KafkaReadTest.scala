package com.streamingkmeans.utils.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * @Author: ch
 * @Date: 07/05/2020 12:44 AM
 * @Version 1.0
 * @Describe: 测试从kafka对应的主题读取数据
 */
object KafkaReadTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val props = new Properties
    props.put("bootstrap.servers", "localhost:9092")
    props.put("zookeeper.connect", "localhost:2181")
    props.put("group.id", "metric-group")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") //key 反序列化

    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest") //value 反序列化


    val dataStreamSource: DataStreamSource[String] = env
      .addSource(new FlinkKafkaConsumer011[String]("test", //kafka topic
      new SimpleStringSchema, // String 序列化
      props))
      .setParallelism(1)

    dataStreamSource.print //把从 kafkaUtils中发出的kafka数据 读取到的数据打印在控制台


    env.execute("Flink add data source")
  }
}
