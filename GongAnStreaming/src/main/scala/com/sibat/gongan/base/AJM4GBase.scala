package com.sibat.gongan.base

import com.sibat.gongan.util.EZJSONParser
import com.sibat.gongan.imp.{Core,ESQueryTrait,CommonCoreTrait,IPropertiesTrait,StatusTrait}


object AJM4GBase extends Core with ESQueryTrait with IPropertiesTrait with CommonCoreTrait{

  case class A4G(lte_dev_code:java.lang.String,imsi:java.lang.String,cap_time:java.lang.String,recieveTime:java.lang.String)

  def parseClass(arr : List[String]):Option[Any] = {
    try{
        Some(A4G(arr(0),arr(1),arr(2),arr(3)))
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
    val a4g = (p+",end").split(",") match {
      case arr:Array[String] if( arr.size == 5 ) => parseClass(arr.toList) match {
                                                                                    case Some(a4g) => a4g.asInstanceOf[A4G]
                                                                                    case _ => return List(PARSEERROR)
                                                                                  }
      case _ => return List(PARSEERROR)
    }

    // make sure hit the id
    val warning = scala.collection.mutable.ListBuffer[String]()
    queryFound(PERSONALINDEX,ESTYPE,"imsi",a4g.imsi) match {
      case Some(id) => warning += List("ajm","imsi",a4g.imsi,id._1,a4g.cap_time,a4g.lte_dev_code,id._2).mkString(",")
      case None =>
    }
    // if (warning.size != 0) {
    //   return warning.toList
    // }
    //
    // queryFound(IMSIINDEX,ESTYPE,"imsi",a4g.imsi) match {
    //   case Some(id) => warning += (List(IMSIINDEX,ESTYPE,"imsi",id).mkString(",")+","+a4g.toString)
    //   case None =>
    // }
    warning.toList
  }

}
