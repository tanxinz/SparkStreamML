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
    props.load(new FileInputStream("common.properties"))
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

   def INPUTTOPICS = GET("inTopics")

   def ZOOKEEPERPORT = GET("zookeeperport")

   def ZOOKEEPEQUORUM = GET("quorum")

   def ESNODES = GET("ESnodes")

   def ESTRANSPORTCLIENTPORT = GET("transportclientport")

   def ESINDEX = GET("ESIndex")

   def ESTYPE = GET("ESType")

   def ESCLUSTER = GET("ESCluster")

   def ESPORT = GET("ESPort")

   def POSTGRESIP = GET("PostgresIP")

  def POSTGRESPORT = GET("PostgresPort")

  def POSTGRESTABLE = GET("PostgresTable")

  def POSTGRESDATABASE = GET("PostgresDataBase")

  def POSTGRESUSER = GET("PostgresUser")

  def POSTGRESPASSWD = GET("PostgresPasswd")

  def CLOSETIME = GET("closeTime")

  def LAST_TIME_IN_METRO_STATION = GET("LastInStationTime")

  def HISTORYATTENUATION = GET("historyAttenuation")

}
