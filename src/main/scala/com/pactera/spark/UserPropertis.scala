package com.pactera.spark

import java.io.FileInputStream
import java.util.Properties

import com.pactera.test.Test
/**
  * Author: xinghua
  * param: method
  * Date 2018-08-15 0:44
  **/
object UserPropertis {

  def getKeyValue(string: String):String = {
    val prop = new Properties()
    val inputStream = Test.getClass.getClassLoader.getResourceAsStream("user.properties")
    prop.load(inputStream)
    val sparkValue = prop.get(string).toString
    sparkValue
  }
}
