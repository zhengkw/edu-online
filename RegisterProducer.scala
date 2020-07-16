package com.zhengkw.onlineDemo.producer

import com.zhengkw.onlineDemo.util.MyKafkaUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName:RegisterProducer
 * @author: zhengkw
 * @description: 读取文件后将文件数据写入到kafka
 *               实现自定义生产者
 * @date: 20/06/29下午 9:31
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object RegisterProducer {
  //主题
  private val topic = "register_topic"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("registerProducer")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    //路径为hdfs路径！
    //  sc.textFile("/zhengkw/register.log",10)
    //指定10个分区
    sc.textFile("file:///E:\\education-online\\com_atguigu_sparkstreaming\\src\\main\\resources\\register.log", 10)
      //在每个分区里操作
      .foreachPartition(partition => {
        //创建一个producer对象
        val producer = new KafkaProducer[String, String](MyKafkaUtil.getKafkaProps)
        partition.foreach(item => {
          //创建ProducerRecord将数据封装
          val msg = new ProducerRecord[String, String](topic, item)
          //发送数据
          producer.send(msg)
        })
        // Flushing accumulated records in producer
        producer.flush()
        //发送完关闭生产者
        producer.close()
      })

  }

}
