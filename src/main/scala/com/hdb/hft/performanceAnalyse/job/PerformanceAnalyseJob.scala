package com.hdb.hft.performanceAnalyse.job

import com.hdb.framework.BaseJob
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
  * description:
  *
  * created by lianghanchang on 2020/5/25
  */
class PerformanceAnalyseJob(spark: SparkSession, serviceConfMap: mutable.HashMap[String, String])
  extends BaseJob(spark: SparkSession, serviceConfMap: mutable.HashMap[String, String]){
  override def run(): Unit = {

    val sqlContext: SQLContext = spark.sqlContext

    //加载XML数据
    readXml("/sql/performanceAnalyse/performanceAnalyse.xml")

    //缓存v_organization表
    sparkSQL(serviceConfMap.getOrElse("v_organization_cache", ""))
    //缓存broker_member_level
    sparkSQL(serviceConfMap.getOrElse("broker_member_level_cache", ""))
    //缓存broker_member_level会员等级表
    sparkSQL(serviceConfMap.getOrElse("bi_kh_manager_cache", ""))
    //缓存会员经理与会员关系表
    sparkSQL(serviceConfMap.getOrElse("bi_kh_employee_cache", ""))

    //缓存经纪人信息认证表到磁盘
    val approveDS: Dataset[Row] = sparkSQL(serviceConfMap.getOrElse("broker_approve_select", "")).repartition(300).persist(StorageLevel.DISK_ONLY)
    approveDS.createOrReplaceTempView("broker_approve_cache")
    //缓存经纪人信息表到磁盘
    val brokerDS: Dataset[Row] = sparkSQL(serviceConfMap.getOrElse("broker_select", "")).repartition(300).persist(StorageLevel.DISK_ONLY)
    brokerDS.createOrReplaceTempView("broker_cache")

    //生成会员经理宽表
    sparkSQL("drop table if exists hft.dm_yx_basic_temp_manager_member_spark")
    sparkSQL(serviceConfMap.getOrElse("dm_yx_basic_temp_manager_member", ""))

    //缓存broker与broker_approve关联表
    sparkSQL(serviceConfMap.getOrElse("broker_join_approve_cache", ""))

    //缓存会员经理中间宽带与organization关联表
    sparkSQL(serviceConfMap.getOrElse("manager_member_join_org_cache", ""))

    //部门业绩报表01
    sparkSQL("drop table if exists hft.dw_depart_achievem_rep01_spark")
//    sparkSQL(serviceConfMap.getOrElse("dw_depart_achievem_rep01", ""))

    //缓存推荐行为表
    sparkSQL(serviceConfMap.getOrElse("client_building_relation_cache", ""))
    //缓存成交行为表
    sparkSQL(serviceConfMap.getOrElse("client_building_order_cache", ""))

    //部门业绩报表02
    sparkSQL("drop table if exists hft.dw_depart_achievem_rep02_spark")
//    sparkSQL(serviceConfMap.getOrElse("dw_depart_achievem_rep02", ""))

    //缓存推荐签约表
    sparkSQL(serviceConfMap.getOrElse("client_building_sale_cache", ""))
    //缓存访问行为表
    sparkSQL(serviceConfMap.getOrElse("client_building_visited_cache", ""))

    //member_manager_achievem_rep
    sparkSQL("drop table if exists hft.member_manager_achievem_rep_spark")
//    sparkSQL(serviceConfMap.getOrElse("member_manager_achievem_rep", ""))

    sparkSQL(serviceConfMap.getOrElse("membg_assistant_rep_temp_cache", ""))

    //释放缓存
    approveDS.unpersist()
    brokerDS.unpersist()
    sqlContext.uncacheTable("v_organization_cache")
    sqlContext.uncacheTable("broker_member_level_cache")
    sqlContext.uncacheTable("bi_kh_manager_cache")
    sqlContext.uncacheTable("bi_kh_employee_cache")
    sqlContext.uncacheTable("broker_join_approve_cache")
    sqlContext.uncacheTable("client_building_relation_cache")
    sqlContext.uncacheTable("client_building_order_cache")
    sqlContext.uncacheTable("client_building_sale_cache")
    sqlContext.uncacheTable("client_building_visited_cache")

  }
}
