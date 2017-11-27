package com.sibat.gongan.base
/****
  天彦电子围栏IMSI数据
***/

import com.sibat.gongan.imp.{Core,ESQueryTrait,CommonCoreTrait,IPropertiesTrait,StatusTrait}

object TYMACBase extends Core with ESQueryTrait with IPropertiesTrait with CommonCoreTrait with StatusTrait{

  case class MAC(deviceId:java.lang.String,time:java.lang.String,status:java.lang.Integer,mac:java.lang.String,recieveTime:java.lang.String)

  def parseClass(arr : List[String]):Option[Any] = {
    try{
      Some(MAC(arr(0),arr(1),arr(2).toInt,arr(3),arr(4)))
    }
    catch{
      case ex:Throwable=> None
    }
  }

  def Alarm(p:String):List[String] = {
    //加上end，scala的split不会分割出最后一直为空的逗号
    val mac = (p+",end").split(",") match {
      case arr:Array[String] if( arr.size == 6 ) => parseClass(arr.toList) match {
                                                                                case Some(mac) => mac.asInstanceOf[MAC]
                                                                                case _ => return List(PARSEERROR)
                                                                              }
      case _ => return List(PARSEERROR)
    }
    // make sure hit the id
    val warning = scala.collection.mutable.ListBuffer[String]()
    if(!mac.mac.equals("")){
      queryFound(PERSONALINDEX,ESTYPE,"mac",mac.mac) match {
        case Some(id) => warning += List("ty","mac",mac.mac,id._1,mac.time,mac.deviceId,id._2).mkString(",")
        case None =>
      }
    }
    // if (warning.size != 0) {
    //   return warning.toList
    // }
    //
    // queryFound(MACINDEX,ESTYPE,"mac",mac.mac) match {
    //   case Some(id) => warning += (List(MACINDEX,ESTYPE,"mac",id).mkString(",")+","+mac.toString)
    //   case None =>
    // }
    warning.toList
  }

}
