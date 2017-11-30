package com.sibat.gongan.base

import com.sibat.gongan.imp.{Core,ESQueryTrait,CommonCoreTrait,IPropertiesTrait,StatusTrait}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame


object APPointBase extends Core with ESQueryTrait with IPropertiesTrait with CommonCoreTrait{

  case class Point(mac:java.lang.String,bid:java.lang.String,fid:java.lang.String,aid:java.lang.String,apid:java.lang.String,
    stime:java.lang.String,longtitude:java.lang.String,latitude:java.lang.String,recieveTime:java.lang.String)

  def parseClass(arr : List[String]):Option[Any] = {
    try{
        Some(Point(long2Mac(arr(0)),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8)))
        }
    catch{
            case ex:Throwable => None
        }
  }

  def long2Mac(long:String) = {
    try{
      long.toLong.toHexString.toUpperCase()
    } catch {
      case e :Exception => long
    }
  }

  def Alarm(data:DataFrame,p:Broadcast[DataFrame]):List[String] = {
    // make sure hit the id
    val warning = scala.collection.mutable.ListBuffer[String]()

    warning ++= InnerJoin(data,p,"mac","mac").select(p.value("mac"),p.value("idno"),data("stime"),data("apid"),p.value("zdrystate"),data("bid")).rdd.map("ap,mac,"+_.mkString(",")).collect

    warning.toList
  }

  // /****
  //   return List
  //   esindex,estype,escol,esid,
  // ****/
  // def Alarm(p:String):List[String] = {
  //   //加上end，scala的split不会分割出最后一直为空的逗号
  //   val ap = (p+",end").split(",") match {
  //     case arr:Array[String] if( arr.size == 10 ) => parseClass(arr.toList) match {
  //                                                                                   case Some(ap) => ap.asInstanceOf[Point]
  //                                                                                   case _ => return List(PARSEERROR)
  //                                                                                 }
  //     case _ => return List(PARSEERROR)
  //   }
  //
  //   // make sure hit the id
  //   val warning = scala.collection.mutable.ListBuffer[String]()
  //   if(!ap.mac.equals("")){
  //     queryFound(PERSONALINDEX,ESTYPE,"mac",ap.mac) match {
  //       case Some(id) => warning += List("ap","mac",ap.mac,id._1,ap.stime,ap.apid,ap.bid,id._2).mkString(",")
  //       case None =>
  //     }
  //   }
  //   // if (warning.size != 0) {
  //   //   return warning.toList
  //   // }
  //   //
  //   // queryFound(MACINDEX,ESTYPE,"mac",ap.mac) match {
  //   //   case Some(id) => warning += (List(MACINDEX,ESTYPE,"mac",id).mkString(",")+","+ap.toString)
  //   //   case None =>
  //   // }
  //   warning.toList
  // }

}
