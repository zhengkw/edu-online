package com.zhengkw.onlineDemo.util

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource


/**
 * @ClassName:DataSourceUtil
 * @author: zhengkw
 * @description:
 * @date: 20/06/29下午 5:51
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object DataSourceUtil {
  private var dataSource: DataSource = _
  private var conn: Connection = _


  val props = new Properties

  /**
   * @descrption: 获取连接对象
   * @return: java.sql.Connection
   * @date: 20/06/30 下午 11:09
   * @author: zhengkw
   */
  def getConnection = {
    try {
      props.setProperty("url", PropertyUtil.getProperty("comerce.properties", "jdbc.url"))
      props.setProperty("username", PropertyUtil.getProperty("comerce.properties", "jdbc.user"))
      props.setProperty("password", PropertyUtil.getProperty("comerce.properties", "jdbc.password"))
      props.setProperty("initialSize", PropertyUtil.getProperty("Druid.properties", "druid.initialSize")) //初始化大小
      props.setProperty("maxActive", PropertyUtil.getProperty("Druid.properties", "druid.maxActive")) //最大连接
      props.setProperty("minIdle", PropertyUtil.getProperty("Druid.properties", "druid.minIdle")) //最小连接
      props.setProperty("maxWait", PropertyUtil.getProperty("Druid.properties", "druid.maxWait")) //等待时长
      props.setProperty("timeBetweenEvictionRunsMillis", PropertyUtil.getProperty("Druid.properties", "druid.timeBetweenEvictionRunsMillis")) //配置多久进行一次检测,检测需要关闭的连接 单位毫秒
      props.setProperty("minEvictableIdleTimeMillis", PropertyUtil.getProperty("Druid.properties", "druid.minEvictableIdleTimeMillis")) //配置连接在连接池中最小生存时间 单位毫秒
      props.setProperty("maxEvictableIdleTimeMillis", PropertyUtil.getProperty("Druid.properties", "druid.maxEvictableIdleTimeMillis")) //配置连接在连接池中最大生存时间 单位毫秒
      props.setProperty("validationQuery", PropertyUtil.getProperty("Druid.properties", "druid.validationQuery"))
      props.setProperty("testWhileIdle", PropertyUtil.getProperty("Druid.properties", "druid.testWhileIdle"))
      props.setProperty("testOnBorrow", PropertyUtil.getProperty("Druid.properties", "druid.testOnBorrow"))
      props.setProperty("testOnReturn", PropertyUtil.getProperty("Druid.properties", "druid.testOnReturn"))
      props.setProperty("keepAlive", PropertyUtil.getProperty("Druid.properties", "druid.keepAlive"))
      props.setProperty("phyMaxUseCount", PropertyUtil.getProperty("Druid.properties", "druid.phyMaxUseCount"))

      dataSource = DruidDataSourceFactory.createDataSource(props)
      conn = dataSource.getConnection()
      conn
    } catch {
      case e => e.getStackTrace
        null
    }
  }


  /**
   * @descrption: 关闭资源
   * @param resultSet
   * @param preparedStatement
   * @param connection
   * @return: java.lang.Object
   * @date: 20/06/29 下午 6:15
   * @author: zhengkw
   */
  def closeResource(resultSet: ResultSet, preparedStatement: PreparedStatement,
                    connection: Connection) = {
    // 关闭结果集
    // ctrl+alt+m 将java语句抽取成方法
    closeResultSet(resultSet)
    // 关闭语句执行者
    closePrepareStatement(preparedStatement)
    // 关闭连接
    closeConnection(connection)
  }

  def closeResultSet(resultSet: ResultSet) = {
    if (resultSet != null) {
      try {
        resultSet.close()
      } catch {
        case e =>
      }
    }
  }

  def closePrepareStatement(preparedStatement: PreparedStatement) = {
    if (preparedStatement != null) {
      try {
        preparedStatement.close()
      } catch {
        case e =>
      }
    }
  }

  def closeConnection(conn: Connection) = {
    if (conn != null)
      try {
        conn.close()
      } catch {
        case exception: Exception =>
      }
  }

}
