package com.hdb.framework

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  * description:
  *
  * created by lianghanchang on 2020/5/22
  */
abstract class BaseJob(spark: SparkSession, serviceConfMap: mutable.HashMap[String, String]) extends JobCommon {
  //任务启动方法
  def run(): Unit

  /**
    * 解析xml并将其保存进serviceConfMap中
    *
    * @param path 本地xml地址
    * @return
    */
  def readXml(path: String) = {
    //读取xml获取参数
    val document = parse(path)
    serviceConfMap.++=(loadServiceConf(document))
  }

  /**
    * 打印sql语句，并执行sql
    *
    * @param sql
    * @return
    */
  def sparkSQL(sql: String): DataFrame = {

    //获取日志级别
    val logLevel = serviceConfMap.getOrElse("logLevel", "INFO")
    logLevel.toUpperCase() match {
      case "WARN" => logWarning(sql)
      case "INFO" => logInfo(sql)
      case "DEBUG" => logDebug(sql)
      case "ERROR" => logError(sql)
      case _ => logInfo(sql)
    }
    spark.sql(sql)
  }
}
