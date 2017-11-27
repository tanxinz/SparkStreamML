package com.sibat.gongan.base

import com.sibat.gongan.util.EZJSONParser
import com.sibat.gongan.imp.{Core,ESQueryTrait,CommonCoreTrait,IPropertiesTrait,StatusTrait}


object AJMWIFIBase extends Core with ESQueryTrait with IPropertiesTrait with CommonCoreTrait{

  case class WIFI(device_mac:java.lang.String,cap_time:java.lang.String,collect_ap_mac:java.lang.String,collect_ap_ssid:java.lang.String,
                  dev_addr:java.lang.String,coordinate:java.lang.String,length:java.lang.String,term_mac:java.lang.String,
                  term_type:java.lang.String,recieveTime:java.lang.String)

  def parseClass(arr : List[String]):Option[Any] = {
    try{
        Some(WIFI(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8),arr(9)))
        }
    catch{
            case ex:Throwable => None
        }
  }

  /****
    return List
    esindex,estype,escol,esid,
  ****/
  def Alarm(p:String):List[String] = {
    //加上end，scala的split不会分割出最后一直为空的逗号
    val wifi = (p+",end").split(",") match {
      case arr:Array[String] if( arr.size == 11 ) => parseClass(arr.toList) match {
                                                                                    case Some(wifi) => wifi.asInstanceOf[WIFI]
                                                                                    case _ => return List(PARSEERROR)
                                                                                  }
      case _ => return List(PARSEERROR)
    }

    // make sure hit the id
    val warning = scala.collection.mutable.ListBuffer[String]()
    queryFound(PERSONALINDEX,ESTYPE,"mac",wifi.device_mac) match {
      case Some(id) => warning += List("ajm","mac",wifi.device_mac,id._1,wifi.cap_time,wifi.term_mac,id._2).mkString(",")
      case None =>
    }
    // if (warning.size != 0) {
    //   return warning.toList
    // }
    //
    // queryFound(MACINDEX,ESTYPE,"mac",wifi.device_mac) match {
    //   case Some(id) => warning += (List(MACINDEX,ESTYPE,"mac",id).mkString(",")+","+wifi.toString)
    //   case None =>
    // }
    warning.toList
  }

}
