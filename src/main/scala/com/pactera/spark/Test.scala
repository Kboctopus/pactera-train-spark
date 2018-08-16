package com.pactera.spark

import java.util.Properties


/**
  * Author: xinghua
  * param: 测试读取properties文件
  * Date 2018-08-15 21:44
  **/
object Test {
  def main(args: Array[String]): Unit = {
    val prop = new Properties()
    val inputStream = Test.getClass.getClassLoader.getResourceAsStream("user.properties")
        prop.load(inputStream)
    val sparkMaster: Any = prop.get("mysqlTable")
    println(sparkMaster)
  }

}
