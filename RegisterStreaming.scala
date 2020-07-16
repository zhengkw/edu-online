package com.zhengkw.onlineDemo.streaming


import java.sql.{Connection, ResultSet}

import com.zhengkw.onlineDemo.util.{DataSourceUtil, MyKafkaUtil, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object RegisterStreaming {
  private val groupid = "register_group"
  private val reset = "earliest"
  //可以订阅多个主题，所以用一个String数组封装
  private val topics = Array("register_topic")
  //查官方文档或者直接看Api可知类型 ConsumerStrategies.Subscribe
  //里面需要的传递一个offset是一个map KV类型 TopicPartition, Long
  val offsetMap: mutable.Map[TopicPartition, Long] = new mutable.HashMap[TopicPartition, Long]()

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "zhengkw")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      //启用内部背压机制（默认false）Spark Streaming能够基于当前的批处理调度延迟和处理时间来控制接收速率，
      // 以便系统仅接收与系统可处理的速度一样的速度。
      // 在内部，这可以动态设置接收器的最大接收速率。
      // 此速率由值spark.streaming.receiver.maxRate及其spark.streaming.kafka.maxRatePerPartition 设置的上限
      //   .set("spark.streaming.backpressure.enabled", "true")

      // Spark会StreamingContext在JVM关闭时正常关闭，而不是立即关闭。
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))
    val sparkContext: SparkContext = ssc.sparkContext

    //sparkStreaming对有状态的数据操作，需要设定检查点目录，然后将状态保存到检查点中
    ssc.checkpoint("/zhengkw/sparkstreaming/checkpoint")
    //查询mysql中是否有偏移量
    val sqlProxy = new SqlProxy()
    val client = DataSourceUtil.getConnection
    try {
      sqlProxy.executeQuery(client, "select * from `offset_manager` where groupid=?", Array(groupid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          //遍历结果集
          while (rs.next()) {
            val model = new TopicPartition(rs.getString(2), rs.getInt(3))
            val offset = rs.getLong(4)
            //查询出有偏移量就将数据存放到map中
            offsetMap.put(model, offset)
          }
          rs.close() //关闭游标
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      //关闭所有资源
      sqlProxy.shutdown(client)
    }
    //设置kafka消费数据的参数  判断本地是否有偏移量  有则根据偏移量继续消费 无则重新消费
    val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      //如果没有offset就从头消费
      MyKafkaUtil.getKafkaStreamWithNoOffset(ssc, topics, groupid, reset)
    } else {
      //如果有offset就从上次消费的offset开始消费
      MyKafkaUtil.getKafkaStreamWithOffset(ssc, topics, offsetMap, groupid, reset)
    }

    //stream原始流无法进行使用和打印，会报序列化错误，所以需要做下面的map转换
    val resultDStream = stream.filter(item => item.value().split("\t").length == 3).
      mapPartitions(partitions => {
        partitions.map(item => {
          val line = item.value()
          val arr = line.split("\t")
          val app_name = arr(1) match {
            case "1" => "PC"
            case "2" => "APP"
            case _ => "Other"
          }
          (app_name, 1)
        })
      })
    resultDStream.cache()
    //(PC,1),(PC,1),(APP,1),(Other,1),(APP,1),(Other,1),(PC,1),(APP,1)
    //"=================每6s间隔1分钟内的注册数据================="
    resultDStream.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(60), Seconds(6)).print()
    //"========================================================="

    //"+++++++++++++++++++++++实时注册人数+++++++++++++++++++++++"//状态计算
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum //本批次求和
      val previousCount = state.getOrElse(0) //历史数据

      Some(currentCount + previousCount)
    }
    resultDStream.updateStateByKey(updateFunc).print()
    //"++++++++++++++++++++++++++++++++++++++++++++++++++++++++"


    //    val dsStream = stream.filter(item => item.value().split("\t").length == 3)
    //      .mapPartitions(partitions =>
    //        partitions.map(item => {
    //          val rand = new Random()
    //          val line = item.value()
    //          val arr = line.split("\t")
    //          val app_id = arr(1)
    //          (rand.nextInt(3) + "_" + app_id, 1)
    //        }))
    //    val result = dsStream.reduceByKey(_ + _)
    //    result.map(item => {
    //      val appid = item._1.split("_")(1)
    //      (appid, item._2)
    //    }).reduceByKey(_ + _).print()

    //处理完 业务逻辑后 手动提交offset维护到本地 mysql中
    stream.foreachRDD(rdd => {


      val sqlProxy = new SqlProxy()
      val client = DataSourceUtil.getConnection
      try {
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (or <- offsetRanges) {
          sqlProxy.executeUpdate(client, "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
            Array(groupid, or.topic, or.partition.toString, or.untilOffset))
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(client)
      }
    })


    ssc.start()
    ssc.awaitTermination()
  }

}
