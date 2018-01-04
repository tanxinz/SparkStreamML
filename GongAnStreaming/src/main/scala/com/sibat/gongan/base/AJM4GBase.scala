package com.sibat.gongan.base

import com.sibat.gongan.util.EZJSONParser
import com.sibat.gongan.imp.{Core,ESQueryTrait,CommonCoreTrait,IPropertiesTrait,StatusTrait}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame


object AJM4GBase extends Core with ESQueryTrait with IPropertiesTrait with CommonCoreTrait{

  case class A4G(lte_dev_code:java.lang.String,imsi:java.lang.String,cap_time:java.lang.String,recieveTime:java.lang.String)

  def parseClass(arr : List[String]):Option[Any] = {
    try{
        Some(A4G(arr(0),arr(1),stamp2Time(arr(2)),arr(3)))
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

      warning ++= InnerJoin(data,p,"imsi","imsi").select(p.value("imsi"),p.value("idno"),data("cap_time"),data("lte_dev_code"),p.value("zdrystate")).rdd.map("ajm,imsi,"+_.mkString(",")).collect

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
