package com.sibat.gongan.base
/****
  天彦电子围栏IMSI数据
***/

import com.sibat.gongan.imp.{Core,ESQueryTrait,CommonCoreTrait,IPropertiesTrait,StatusTrait}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame

object TYIMSIBase extends Core with ESQueryTrait with IPropertiesTrait with CommonCoreTrait with StatusTrait{

  case class IMSI(deviceId:java.lang.String,time:java.lang.String,imsi:java.lang.String,imei:java.lang.String,location:java.lang.String,recieveTime:java.lang.String)

  def parseClass(arr : List[String]):Option[Any] = {
    try{
      Some(IMSI(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5)))
    }catch{
      case ex:Throwable=> None
    }
  }

  def Alarm(data:DataFrame,p:Broadcast[DataFrame]):List[String] = {
    // make sure hit the id
    val warning = scala.collection.mutable.ListBuffer[String]()

    warning ++= InnerJoin(data,p,"imsi","imsi").select("imsi","idno","time","deviceId","zdrystate").map("ty,imsi,"+_.mkString(","))

    warning.toList
  }

  // def Alarm(p:String):List[String] = {
  //   //加上end，scala的split不会分割出最后一直为空的逗号
  //   val imsi = (p+",end").split(",") match {
  //     case arr:Array[String] if( arr.size == 7 ) => parseClass(arr.toList) match {
  //                                                                                 case Some(imsi) => imsi.asInstanceOf[IMSI]
  //                                                                                 case _ => return List(PARSEERROR)
  //                                                                                 }
  //     case _ => return List(PARSEERROR)
  //   }
  //   // make sure hit the id
  //   val warning = scala.collection.mutable.ListBuffer[String]()
  //   for ((target,record) <- Map("imsi" -> imsi.imsi,"imei" -> imsi.imei)){
  //     if(!record.equals("")){
  //       queryFound(PERSONALINDEX,ESTYPE,target,record) match {
  //         case Some(id) => warning += List("ty",target,record,id._1,imsi.time,imsi.deviceId,id._2).mkString(",")
  //         case None =>
  //       }
  //     }
  //   }
  //   // if (warning.size != 0) {
  //   //   return warning.toList
  //   // }
  //   //
  //   // queryFound(IMSIINDEX,ESTYPE,"imsi",imsi.imsi) match {
  //   //   case Some(id) => warning += (List(IMSIINDEX,ESTYPE,"imsi",id).mkString(",")+","+imsi.toString)
  //   //   case None =>
  //   // }
  //   //
  //   // queryFound(IMEIINDEX,ESTYPE,"imei",imsi.imei) match {
  //   //   case Some(id) => warning += (List(IMEIINDEX,ESTYPE,"imei",id).mkString(",")+","+imsi.toString)
  //   //   case None =>
  //   // }
  //   warning.toList
  // }


}
