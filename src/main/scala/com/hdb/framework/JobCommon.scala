package com.hdb.framework

import java.io.InputStream
import java.util
import java.util.Properties

import org.apache.spark.internal.Logging
import org.dom4j.io.SAXReader
import org.dom4j.{Document, DocumentException, Element}

import scala.collection.mutable
import scala.xml.SAXException

/**
  * description: 启动类公共接口类
  *
  * created by lianghanchang on 2020/5/22
  */
class JobCommon extends Serializable with Logging {
  //公共变量
  protected val serviceConfMap = new mutable.HashMap[String, String]()

  {
    val properties = new Properties()
    val in: InputStream = this.getClass.getClassLoader.getResourceAsStream("env.properties")
    properties.load(in)
    serviceConfMap.put("mysql_driver", properties.getProperty("mysql.jdbc.driver"))
    serviceConfMap.put("mysql_url", properties.getProperty("mysql.jdbc.url"))
    serviceConfMap.put("mysql_username", properties.getProperty("mysql.jdbc.username"))
    serviceConfMap.put("mysql_password", properties.getProperty("mysql.jdbc.password"))
  }

  /**
    * 对sql中的占位符进行解析
    *
    * @param sql 传入的sql
    * @return
    */
  def selectMapping(sql: String, valueMap: mutable.HashMap[String, String]): String = {
    val iter = valueMap.iterator
    val stringBuffer: StringBuffer = new StringBuffer(sql)
    var replaceSql = ""
    var resultSql = ""

    while (iter.hasNext) {
      val entry: (String, String) = iter.next()
      val key: String = entry._1.trim
      val value: String = entry._2
      replaceSql = stringBuffer.toString.replace("${" + key + "}", value)
      stringBuffer.delete(0, stringBuffer.length())
      stringBuffer.append(replaceSql)
      resultSql = stringBuffer.toString
    }

    resultSql
  }

  /**
    * 创建document
    *
    * @param xmlPath 本地xml文件地址
    * @return
    */
  protected def parse(xmlPath: String): Document = {
    val reader = new SAXReader()
    var document: Document = null

    try {
      reader.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
      reader.setFeature("http://xml.org/sax/features/external-general-entities", false)
      reader.setFeature("http://xml.org/sax/features/external-parameter-entities", false)
      val xml: InputStream = this.getClass.getResourceAsStream(xmlPath)
      document = reader.read(xml)
    } catch {
      case e: DocumentException =>
        log.error("document can not be created: " + e)
      case e: SAXException =>
        log.error("SAXException: " + e)
    }

    document
  }

  /**
    * @param document dom4j操作对象
    * @return map对象
    */
  protected def loadServiceConf(document: Document): mutable.HashMap[String, String] = {
    val root = document.getRootElement
    val iter = root.elementIterator()
    while (iter.hasNext) {
      val element: Element = iter.next().asInstanceOf[Element]
      serviceConfMap.put(element.getName(), element.getText)
    }
    serviceConfMap
  }

  /**
    * 递归查找element的值，插入map对象
    *
    * @param element
    * @param confMap
    */
  protected def findElement(element: Element, confMap: mutable.HashMap[String, String]): Unit = {
    val iterInner: util.Iterator[_] = element.elementIterator
    while (iterInner.hasNext) {
      val stringBuffer = new StringBuffer()
      val element: Element = iterInner.next().asInstanceOf[Element]
      stringBuffer.append(element.getName)
      if (element.elementIterator.hasNext) findElement(element, confMap)
      serviceConfMap.put(element.getName, element.getText)
      val value: String = element.getText
      if (!value.trim.isEmpty) confMap.put(stringBuffer.toString, value.trim)
    }
  }

}
