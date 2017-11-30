package com.sibat.gongan.base
/****
  公安一所感知门身份证
  预警字段：身份证(idno)，MAC
***/

import com.sibat.gongan.imp.{Core,ESQueryTrait,CommonCoreTrait,IPropertiesTrait,StatusTrait}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame

object SensorIdcardBase extends Core with ESQueryTrait with IPropertiesTrait with CommonCoreTrait with StatusTrait{

  case class Idcard(mac:java.lang.String,genId:java.lang.String,timeStamp:java.lang.String,idno:java.lang.String,name:java.lang.String
                    ,sexCode:java.lang.String,nationCode:java.lang.String,
                    birth:java.lang.String,addr:java.lang.String,authority:java.lang.String,validBegin:java.lang.String
                    ,validEnd:java.lang.String,recieveTime:java.lang.String)

  def parseClass(arr : List[String]):Option[Any] = {
    try{
      Some(Idcard(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8),arr(9),arr(10),arr(11),arr(12)))
    }catch{
      case ex:Throwable => None
    }
  }

  def Alarm(data:DataFrame,p:Broadcast[DataFrame]):List[String] = {
    // make sure hit the id
    val warning = scala.collection.mutable.ListBuffer[String]()

    warning ++= InnerJoin(data,p,"idno","idno").select(p.value("idno"),p.value("idno"),data("timeStamp"),data("mac"),p.value("zdrystate"),data("genId")).rdd.map("sensordoor,idno,"+_.mkString(",")).collect

    warning.toList
  }

  // def Alarm(p:String):List[String] = {
  //   //加上end，scala的split不会分割出最后一直为空的逗号
  //   val idcard = (p+",end").split(",") match {
  //     case arr:Array[String] if( arr.size == 15 ) => parseClass(arr.toList) match {
  //                                                                                 case Some(idcard) => idcard.asInstanceOf[Idcard]
  //                                                                                 case _ => return List(PARSEERROR)
  //                                                                                 }
  //     case _ => return List(PARSEERROR)
  //   }
  //
  //   // make sure hit the id
  //   val warning = scala.collection.mutable.ListBuffer[String]()
  //   for ((target,record) <- Map("mac" -> idcard.mac,"idno" -> idcard.idno)){
  //     if(!record.equals("")){
  //       queryFound(PERSONALINDEX,ESTYPE,target,record) match {
  //         case Some(id) => warning += List("sensordoor",target,record,id._1,idcard.timeStamp,idcard.mac,id._2,idcard.genId).mkString(",")
  //         case None =>
  //       }
  //     }
  //   }
  //
  //   // if (warning.size != 0) {
  //   //   return warning.toList
  //   // }
  //   //
  //   // queryFound(MACINDEX,ESTYPE,"mac",idcard.mac) match {
  //     // case Some(id) => warning += (List(MACINDEX,ESTYPE,"mac",id).mkString(",")+","+idcard.toString)
  //     // case None =>
  //   // }
  //   //
  //   // queryFound(IDNOINDEX,ESTYPE,"idno",idcard.idno) match {
  //   //   case Some(id) => warning += (List(IDNOINDEX,ESTYPE,"idno",id).mkString(",")+","+idcard.toString)
  //   //   case None =>
  //   // }
  //   warning.toList
  // }

}
