package com.zhengkw.onlineDemo.util


import java.lang
import java.util.Properties
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable


/**
 * @ClassName:KafkaUtil
 * @author: zhengkw
 * @description: kafka连接
 * @date: 20/06/29下午 5:01
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object MyKafkaUtil {

  /**
   * @descrption: 当offset查询为空时调用此方法
   * @param ssc    StreamingContext
   * @param topics 主题集合
   * @return: org.apache.spark.streaming.dstream.InputDStream<org.apache.kafka.clients.consumer.ConsumerRecord<java.lang.String,java.lang.String>>
   * @date: 20/06/29 下午 5:43
   * @author: zhengkw
   */
  def getKafkaStreamWithNoOffset(ssc: StreamingContext, topics: Array[String], groupId: String, whereStart: String) = {
    val stream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, getKafkaMap(groupId, whereStart)))
    stream
  }

  /**
   * @descrption: 查询mysql返回一个offset，
   *              通过该offset开始消费
   * @param ssc       streamingContext
   * @param topics    主题集合
   * @param offsetMap 参考官网文档
   * @return: org.apache.spark.streaming.dstream.InputDStream<org.apache.kafka.clients.consumer.ConsumerRecord<java.lang.String,java.lang.String>>
   * @date: 20/06/29 下午 5:46
   * @author: zhengkw
   */
  def getKafkaStreamWithOffset(ssc: StreamingContext, topics: Array[String], offsetMap: mutable.Map[TopicPartition, Long], groupId: String, whereStart: String) = {
    val stream = KafkaUtils.createDirectStream(
      ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, getKafkaMap(groupId, whereStart), offsetMap))
    stream
  }

  /**
   * @descrption: 获取配置文件中的配置信息，初始化用
   * @return: java.util.Properties
   * @date: 20/06/29 下午 10:18
   * @author: zhengkw
   */
  def getKafkaProps = {
    val props = new Properties()
    props.put("bootstrap.servers", PropertyUtil.getProperty("kafka.properties", "kafka.bootstrap.servers"))
    props.put("acks", PropertyUtil.getProperty("kafka.properties", "kafka.acks"))
    props.put("batch.size", PropertyUtil.getProperty("kafka.properties", "kafka.batch.size")) //kafka生产者批量发送的的缓存大小，默认是16kB
    props.put("linger.ms", PropertyUtil.getProperty("kafka.properties", "kafka.linger.ms")) //kafka生产者发送延时，默认延时时间是0s，接到消息立刻发送
    props.put("buffer.memory", PropertyUtil.getProperty("kafka.properties", "kafka.buffer.memory")) //kafka生产者用于缓存消息的缓冲区的大小，单位是字节，默认32M
    props.put("key.serializer",
      PropertyUtil.getProperty("kafka.properties", "kafka.key.serializer"))
    props.put("value.serializer",
      PropertyUtil.getProperty("kafka.properties", "kafka.value.serializer"))
    props
  }

  def getKafkaMap(groupId: String, whereStart: String) = {
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" ->
        PropertyUtil.getProperty("kafka.properties", "kafka.bootstrap.servers"),
      "key.deserializer" -> classOf[StringDeserializer]
      ,
      "value.deserializer" -> classOf[StringDeserializer]
      ,
      "group.id" -> groupId
      ,
      "auto.offset.reset" -> whereStart
      , //sparkstreaming第一次启动，不丢数
      //如果是true，则这个消费者的偏移量会在后台自动提交，但是kafka宕机容易丢失数据
      //如果是false，则需要手动维护kafka偏移量
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    kafkaMap
  }

}
