package com.zhengkw.onlineDemo.util

import java.sql.{Connection, PreparedStatement, ResultSet}

/**
 * @descrption: 定义一个抽象方法process
 *              接受一个 结果集 具体实现需要调用时实现
 * @date: 20/06/29 下午 9:24
 * @author: zhengkw
 */
trait QueryCallback {
  def process(rs: ResultSet)
}

class SqlProxy {
  private var rs: ResultSet = _
  private var psmt: PreparedStatement = _

  /**
   * 执行修改语句
   *
   * @param conn   连接对象
   * @param sql    执行的SQL
   * @param params 占位符需要的具体数据
   * @return 影响条数
   */
  def executeUpdate(conn: Connection, sql: String, params: Array[Any]): Int = {
    var rtn = 0
    try {
      psmt = conn.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          psmt.setObject(i + 1, params(i))
        }
      }
      rtn = psmt.executeUpdate()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  /**
   * @descrption: 执行查询语句
   * @param conn          连接对象
   * @param sql           执行的SQL
   * @param params        占位符需要的具体数据
   * @param queryCallback 回调函数
   * @return: void
   * @date: 20/06/29 下午 9:23
   * @author: zhengkw
   */
  def executeQuery(conn: Connection, sql: String, params: Array[Any], queryCallback: QueryCallback) = {
    rs = null
    try {
      psmt = conn.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          //用具体值覆盖占位符
          psmt.setObject(i + 1, params(i))
        }
      }
      rs = psmt.executeQuery()
      queryCallback.process(rs)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def shutdown(conn: Connection): Unit = DataSourceUtil.closeResource(rs, psmt, conn)
}
