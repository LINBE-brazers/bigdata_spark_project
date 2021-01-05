package com.hdb.common.utils

import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * description: SparkSession初始化公共类
  *
  * created by lianghanchang on 2020/5/22
  */
object SparkUtils {

  /**
    * 实例化SparkSession
    *
    * @param logLevel
    * @param appName
    * @return
    */
  def createSparkSession(logLevel: String, appName: String): SparkSession = {
    createSparkSession(logLevel, appName, null)
  }

  /**
    * 实例化SparkSession
    *
    * @param logLevel 打印日志级别
    * @param appName  spark任务名
    * @return
    */
  def createSparkSession(logLevel: String, appName: String, classArr: Array[Class[_]]): SparkSession = {

    val sparkConf = new SparkConf()
      .setAppName(appName)
      .set("spark.rdd.compress", "true")
      .set("spark.sql.codegen", "true")
      .set("spark.sql.inMemoryColumnStorage.compress", "true")
      .set("hive.execution.engine", "spark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") /*kryo序列化*/

    if (classArr != null && classArr.length > 0) {
      sparkConf.registerKryoClasses(classArr)
    }

    val spark =  SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //设置日志打印级别
    logLevel.toUpperCase() match {
      case "WARN" => spark.sparkContext.setLogLevel(Level.WARN.toString)
      case "INFO" => spark.sparkContext.setLogLevel(Level.INFO.toString)
      case "DEBUG" => spark.sparkContext.setLogLevel(Level.DEBUG.toString)
      case "ERROR" => spark.sparkContext.setLogLevel(Level.ERROR.toString)
      case _ => spark.sparkContext.setLogLevel(Level.INFO.toString)
    }
    spark
  }

}
