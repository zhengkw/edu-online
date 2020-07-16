package com.zhengkw.onlineDemo.util

import com.alibaba.fastjson.JSON

/**
 * @ClassName:JsonParseUtil
 * @author: zhengkw
 * @description:
 * @date: 20/06/30下午 3:40
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object JsonParseUtil {
  //
  def getJsonObject(data: String) = {
    //json字符串转json对象
    try {
      JSON.parseObject(data)
    } catch {
      case e => null
    }
  }
}
