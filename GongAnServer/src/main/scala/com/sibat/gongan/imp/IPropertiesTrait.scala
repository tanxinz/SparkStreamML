package com.sibat.gongan.imp

import java.util.Properties
import java.lang.System
import java.io.FileInputStream
import scala.collection.mutable.Map

trait IPropertiesTrait {

  private val conf = Map[String,String]()

  /**************
    初始化配置
  *************/
  private def InitProperties(): Unit = {
    val filePath =System.getProperty("user.dir")
    val props = new Properties()
    props.load(new FileInputStream(filePath+"/common.properties"))
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

   def SPARKMASTER = GET("master")

   def APPNAME = GET("appName")

   def KAFKABROKERS = GET("brokers")

   def INPUTTOPICS = GET("inTopics")

   def WARNTOPICS = GET("warnTopics")

   def WARNTEMPTOPICS = GET("warnTempTopics")

   def KAFKAPORT = GET("kafkaport")

   def ZOOKEEPERPORT = GET("zookeeperport")

   def ZOOKEEPEQUORUM = GET("quorum")

   def HBASETABLENAME = GET("tablename")

   def ESNODES = GET("ESnodes")

   def ESTRANSPORTCLIENTPORT = GET("transportclientport")

   def ESINDEX = GET("ESIndex")

   def ESTYPE = GET("ESType")

   def ESCLUSTER = GET("ESCluster")

   def PERSONALINDEX = GET("ESPersonalIndex")

   def MACINDEX = GET("ESMacIndex")

   def IMSIINDEX = GET("ESIMSIIndex")

   def IMEIINDEX = GET("ESIMEIIndex")

   def PHONEINDEX = GET("ESPhoneIndex")

   def SZTINDEX = GET("ESSZTIndex")

   def ACCOUNTINDEX = GET("ESAccountIndex")

   def IDNOINDEX = GET("ESIdNoIndex")
}
