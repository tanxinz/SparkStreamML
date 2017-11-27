package com.sibat.gongan.imp

import org.apache.hadoop.hbase.client.Put

//execute Core
abstract class Core{

  case class CoreClass(dataid1:String,time1:String)
  case class Core2Class(dataid2:String,time2:String)

  /*********
    query from ES to make sure it's a safe recoder
    define a alarm info and send it to kafka( or a cache
    cause that a nearly alarm happended in the short time
    and will send lots of same info to web)
  *********/
  // def Alarm(recorder:String):Map[String,String]

  /**********
    make the recorder to PUT to save it to hbase soon
  **********/
  // def FormatPut(alarm:Map[String,String]):Put


  // def ESDecoder(context:String):Map[String,Any]

}
