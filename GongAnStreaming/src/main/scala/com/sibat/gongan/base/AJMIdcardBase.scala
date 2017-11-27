package com.sibat.gongan.base

import com.sibat.gongan.util.EZJSONParser
import com.sibat.gongan.imp.{Core,ESQueryTrait,CommonCoreTrait,IPropertiesTrait,StatusTrait}


object AJMIdcardBase extends Core with ESQueryTrait with IPropertiesTrait with CommonCoreTrait{

  case class Idcard(name:java.lang.String,sex:java.lang.String,nation:java.lang.String,birth:java.lang.String,
                  addr:java.lang.String,idno:java.lang.String,authority:java.lang.String,validBegin:java.lang.String,validEnd:java.lang.String,
                  face:java.lang.String,cap_time:String,recieveTime:java.lang.String)

  def parseClass(arr : List[String]):Option[Any] = {
    try{
        Some(Idcard(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8),arr(9),arr(10),arr(11)))
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
    val idcard = (p+",end").split(",") match {
      case arr:Array[String] if( arr.size == 13 ) => parseClass(arr.toList) match {
                                                                                    case Some(x) => x.asInstanceOf[Idcard]
                                                                                    case _ => return List(PARSEERROR)
                                                                                  }
      case _ => return List(PARSEERROR)
    }

    // make sure hit the id
    val warning = scala.collection.mutable.ListBuffer[String]()
    queryFound(PERSONALINDEX,ESTYPE,"idno",idcard.idno) match {
      case Some(id) => warning += List("ajm","idno",id._1,idcard.cap_time,idcard.addr,id._2).mkString(",")
      case None =>
    }
    // if (warning.size != 0) {
    //   return warning.toList
    // }
    //
    // queryFound(IDNOINDEX,ESTYPE,"idno",idcard.idno) match {
    //   case Some(id) => warning += (List(IDNOINDEX,ESTYPE,"idno",id).mkString(",")+","+idcard.toString)
    //   case None =>
    // }
    warning.toList
  }

}
