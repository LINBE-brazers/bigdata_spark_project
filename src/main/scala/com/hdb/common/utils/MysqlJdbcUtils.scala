package com.hdb.common.utils

import java.sql.{Connection, DriverManager}
import java.util.Properties

import scala.collection.mutable

/**
  * description: JDBC连接mysql公共类
  *
  * created by lianghanchang on 2020/5/22
  */
object MysqlJdbcUtils {
  private val DRIVER = "com.mysql.jdbc.Driver" //Mysql驱动
  private var mysql_url = "" //mysql连接串
  private var mysql_username = "" //mysql登陆用户名
  private var mysql_password = "" //mysql登陆密码


  /**
    * 获取mysql连接资源
    *
    * @param serviceConfMap
    * @return
    */
  def getConnection(serviceConfMap: mutable.HashMap[String, String]): Connection = {
    argsCheck(serviceConfMap)
    getConnection(mysql_url, mysql_username, mysql_password)
  }


  /**
    * 获取mysql连接资源
    *
    * @param mysql_url  mysql连接串
    * @param mysql_username 用户名
    * @param mysql_password 密码
    * @return
    */
  def getConnection(
                     mysql_url: String,
                     mysql_username: String,
                     mysql_password: String
                   ): Connection = {
    if (mysql_url.isEmpty || mysql_username.isEmpty || mysql_password.isEmpty) {
      throw new IllegalArgumentException("mysql连接串或者用户名或者密码不能为空")
    }
    Class.forName(DRIVER)
    DriverManager.getConnection(mysql_url, mysql_username, mysql_password)
  }

  /**
    * 获取properties
    *
    * @param serviceConfMap
    * @return
    */
  def getProperties(serviceConfMap: mutable.HashMap[String, String]): Properties = {
    val mysql_username = serviceConfMap.getOrElse("mysql_username", "")
    val mysql_password = serviceConfMap.getOrElse("mysql_password", "")
    if (mysql_username.isEmpty || mysql_password.isEmpty) {
      throw new IllegalArgumentException("参数mysql_username或mysql_password不能为空")
    }
    getProperties(mysql_username, mysql_password)
  }

  /**
    * 获取properties
    *
    * @param mysql_username
    * @param mysql_password
    * @return
    */
  def getProperties(
                   mysql_username: String,
                   mysql_password: String
                   ): Properties = {
    val properties = new Properties()
    properties.put("user", mysql_username)
    properties.put("password", mysql_password)
    properties.put("driver", DRIVER)
    properties
  }


  /**
    * 校验mysql的连接参数，分别为mysql_url，mysql_username， mysql_password
    *
    * @param serviceConfMap
    * @return
    */
  private def argsCheck(serviceConfMap: mutable.HashMap[String, String]): Unit = {
    mysql_url = serviceConfMap.getOrElse("mysql_url", "")
    mysql_username = serviceConfMap.getOrElse("mysql_username", "")
    mysql_password = serviceConfMap.getOrElse("mysql_password", "")
    if (mysql_url.isEmpty || mysql_username.isEmpty || mysql_password.isEmpty) {
      throw new IllegalArgumentException("参数mysql_url或mysql_username或mysql_password不能为空")
    }
  }

}
