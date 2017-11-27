package com.sibat.gongan.imp

import java.util.Properties
import java.io.FileInputStream
import scala.collection.mutable.Map

trait IPropertiesTrait {

  private val conf = Map[String,String]()

  /**************
    初始化配置
  *************/
  private def InitProperties(): Unit = {
    val props = new Properties()
    props.load(new FileInputStream("/home/hadoop/Gongan/Counter/common.properties"))
    props.keySet().toArray().foreach { x =>
                    conf += (x.toString -> props.getProperty(x.toString()))
                }
  }

  /************
    获取配置
  ************/
  private def GET(propname : String)(): String = {
     if(conf.isEmpty){
       InitProperties()
     }
     conf(propname)
   }

   def APPNAME = GET("appName")

   def ZOOKEEPEQUORUM = GET("quorum")

   def ZOOKEEPERPORT = GET("zookeeperport")

   def SZTTABLENAME = GET("szttablename")

   def TYMACTABLENAME = GET("tymactablename")

   def TYIMSITABLENAME = GET("tyimsitablename")

   def HBASEMASTER = GET("hbasemaster")

   def COUNTERTIMESPAN = GET("counterTimeSpan")

   def LOGTIMESPAN = GET("logTimeSpan")

   def ODCOUNTTABLE=GET("odcounttable")

   def SZTCOUNTALLTABLE=GET("sztcountalltablename")

   def AJM4GTABLE=GET("ajm4gtablename")

   def AJMWIFITABLE=GET("ajmwifitablename")

   def AJMACCOUNTTABLE=GET("ajmaccounttablename")

   def APPOINTTABLE=GET("appointtablename")

   def RZXFEATURETABLE=GET("rzxfeaturetablename")

   def SENSORDOORIDCARDTABLE=GET("sensordooridcardtablename")

   def IDNOTABLE=GET("idnotablename")

   def MACTABLE=GET("mactablename")

   def IMSITABLE=GET("imsitablename")

}
