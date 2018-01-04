package com.sibat.gongan.base

import com.sibat.gongan.util.EZJSONParser
import com.sibat.gongan.imp.{Core,ESQueryTrait,CommonCoreTrait,IPropertiesTrait,StatusTrait}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame

object AJMWIFIBase extends Core with ESQueryTrait with IPropertiesTrait with CommonCoreTrait{

  case class WIFI(device_mac:java.lang.String,cap_time:java.lang.String,collect_ap_mac:java.lang.String,collect_ap_ssid:java.lang.String,
                  dev_addr:java.lang.String,coordinate:java.lang.String,length:java.lang.String,term_mac:java.lang.String,
                  term_type:java.lang.String,recieveTime:java.lang.String)

  def parseClass(arr : List[String]):Option[Any] = {
    try{
        Some(WIFI(arr(0),stamp2Time(arr(1)),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8),arr(9)))
        }
    catch{
            case ex:Throwable => None
        }
  }

  /****
    return List
    esindex,estype,escol,esid,
  ****/
  def Alarm(data:DataFrame,p:Broadcast[DataFrame]):List[String] = {
    // make sure hit the id
    val warning = scala.collection.mutable.ListBuffer[String]()

    warning ++= InnerJoin(data,p,"term_mac","mac").select(p.value("mac"),p.value("idno"),data("cap_time"),data("device_mac"),p.value("zdrystate")).rdd.map("ajm,mac,"+_.mkString(",")).collect

    warning.toList
  }

  def stamp2Time(timeStamp:String) = {
    try{
      val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      format.format(new java.util.Date(timeStamp.toLong))
    }catch {
      case e:Exception => timeStamp
    }
  }

}
