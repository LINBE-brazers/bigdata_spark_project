package com.hdb.hft.performanceAnalyse.launcher

import com.hdb.common.utils.SparkUtils
import com.hdb.framework.JobCommon
import com.hdb.hft.performanceAnalyse.job.PerformanceAnalyseJob
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

/**
  * description:
  *
  * created by lianghanchang on 2020/5/25
  */
object PerformanceAnalyseLauncher extends JobCommon {

  def main(args: Array[String]): Unit = {

    //开始时间
    val start = System.currentTimeMillis()

    //解析参数
    args.foreach(arg => {
      logInfo(arg)
      val argArr: Array[String] = arg.split("=")
      if (argArr.length > 1) {
        serviceConfMap.put(argArr(0), argArr(1))
      }
    })

//    val logLevel = serviceConfMap.getOrElse("ERROR", "INFO")

    //实例化SparkSession
    //        val spark: SparkSession = SparkUtils.createSparkSession(logLevel, "PerformanceAnalyseLauncher")
    val spark = SparkSession
      .builder()
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel(Level.ERROR.toString)
    serviceConfMap.put("logLevel", "ERROR")

    //开始计算任务
    new PerformanceAnalyseJob(spark, serviceConfMap).run()

    spark.stop()

    logError("sparkJob time consuming：" + (System.currentTimeMillis() - start) / 1000 + "Seconds")
  }

}
