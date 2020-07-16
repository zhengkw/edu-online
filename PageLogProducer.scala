package com.zhengkw.onlineDemo.producer

import com.zhengkw.onlineDemo.util.MyKafkaUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:PageLogProducer
 * @author: zhengkw
 * @description: 通过自定义生产者，将数据写入到kafka中
 *               类似RegisterProducer操作
 * @date: 20/06/30下午 2:16
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object PageLogProducer {
  private val topic = "page_topic"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("pageProducer").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    // sc.textFile("/zhengkw/page.log",10)
    val sourceRDD = sc.textFile("file:///E:\\IdeaWorkspace\\edu-online\\SparkStreamingModule\\src\\main\\resources\\page.log", 10)
    sourceRDD.foreachPartition(partition => {
      val producer = new KafkaProducer[String, String](MyKafkaUtil.getKafkaProps)
      partition.foreach(item => {
        val msg = new ProducerRecord[String, String](topic, item)
        producer.send(msg)
      })
      producer.flush()
      producer.close()
    })
  }
}
