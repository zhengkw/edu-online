package com.zhengkw.onlineDemo.streaming

import java.sql.ResultSet

import com.alibaba.fastjson.JSONObject
import com.zhengkw.onlineDemo.util.{DataSourceUtil, JsonParseUtil, MyKafkaUtil, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @ClassName:PageStreaming
 * @author: zhengkw
 * @description:
 * @date: 20/06/30下午 2:58
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object PageStreaming {
  private val topics = Array("page_topic")
  private val groupId = "page_groupid"
  private val reset = "earliest"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "30")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .setMaster("local[*]")
    //创建StreamingContext 3s一个批次
    val ssc = new StreamingContext(conf, Seconds(3))
    //获取kafkaMap 里面是配置信息，创建直连流用
    val kafkaMap = MyKafkaUtil.getKafkaMap(groupId, reset)
    //查询mysql中是否存在偏移量
    val sqlProxy = new SqlProxy()
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val conn = DataSourceUtil.getConnection
    try {
      sqlProxy.executeQuery(conn, "select *from `offset_manager` where groupid=?", Array(groupId), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          //遍历结果集
          while (rs next()) {
            val topicPartition = new TopicPartition(rs.getString(2), rs.getInt(3))
            val offset = rs.getLong(4)
            offsetMap.put(topicPartition, offset)
          }
          rs.close()
        }
      })
    } catch {
      case e => e.getStackTrace
    } finally {
      sqlProxy.shutdown(conn)
    }
    //设置kafka消费数据的参数
    // 判断本地是否有偏移量
    // 有则根据偏移量继续消费 无则重新消费
    val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      MyKafkaUtil.getKafkaStreamWithNoOffset(ssc, topics, groupId, reset)
    } else {
      MyKafkaUtil.getKafkaStreamWithOffset(ssc, topics, offsetMap, groupId, reset)
    }
    val dsStream = stream.map(item => item.value())
      .filter(item => {
        val obj = JsonParseUtil.getJsonObject(item)
        //如果转换时出了异常返回的是null，所以要过滤掉null
        obj.isInstanceOf[JSONObject]
      })
      .mapPartitions(partition => {
        partition.map(item => {
          //获取json对象
          val jsonObject = JsonParseUtil.getJsonObject(item)
          //字段解析
          val uid = if (jsonObject.containsKey("uid")) jsonObject.getString("uid") else ""
          val app_id = if (jsonObject.containsKey("app_id")) jsonObject.getString("app_id") else ""
          val device_id = if (jsonObject.containsKey("device_id")) jsonObject.getString("device_id") else ""
          val ip = if (jsonObject.containsKey("ip")) jsonObject.getString("ip") else ""
          val last_page_id = if (jsonObject.containsKey("last_page_id")) jsonObject.getString("last_page_id") else ""
          val pageid = if (jsonObject.containsKey("page_id")) jsonObject.getString("page_id") else ""
          val next_page_id = if (jsonObject.containsKey("next_page_id")) jsonObject.getString("next_page_id") else ""
          (uid, app_id, device_id, ip, last_page_id, pageid, next_page_id)

        })
      })
    //todo
    ssc.start()
    ssc.awaitTermination()
  }
}
