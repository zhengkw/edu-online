package com.zhengkw.onlineDemo.producer

import java.util.Properties

import com.zhengkw.onlineDemo.util.MyKafkaUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:QzLogProducer
 * @author: zhengkw
 * @description:
 * @date: 20/06/30下午 4:16
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object QzLogProducer {
  private val topic = "qz_log"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("logProducer").setMaster("local[*]")
    val ssc = new SparkContext(sparkConf)
    //    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common-2.2.0-bin-master")
    //    val resultLog = ssc.textFile("file://"+this.getClass.getResource("/qz.log").toURI.getPath, 10)
    val resultLog = ssc.textFile("file:///E:\\IdeaWorkspace\\edu-online\\SparkStreamingModule\\src\\main\\resources\\qz.log", 10)
      .foreachPartition(partitoin => {

        val producer = new KafkaProducer[String, String](MyKafkaUtil.getKafkaProps)
        partitoin.foreach(item => {
          val msg = new ProducerRecord[String, String](topic, item)
          producer.send(msg)
        })
        producer.flush()
        producer.close()
      })
  }
}
