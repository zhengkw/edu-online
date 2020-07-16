package com.zhengkw.onlineDemo.util

import java.util.Properties

/**
 * @ClassName:PropertyUtil
 * @author: zhengkw
 * @description:
 * @date: 20/06/29下午 5:30
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object PropertyUtil {
  /**
   * 属性文件
   *
   * @param fileName     属性文件
   * @param propertyName 属性名
   */
  def getProperty(fileName: String, propertyName: String) = {
    // 1. 读取文件内容
    val is = PropertyUtil.getClass.getClassLoader.getResourceAsStream(fileName)
    val properties = new Properties()
    properties.load(is)
    // 2. 根据属性名得到属性值
    properties.getProperty(propertyName)
  }
}
