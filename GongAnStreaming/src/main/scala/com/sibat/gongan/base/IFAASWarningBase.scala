package com.sibat.gongan.base

import com.sibat.gongan.util.EZJSONParser
import com.sibat.gongan.imp.{Core,ESQueryTrait,CommonCoreTrait,IPropertiesTrait,StatusTrait}


object IFAASWarningBase extends Core with ESQueryTrait with IPropertiesTrait with CommonCoreTrait{

  case class Warning(alarmId:java.lang.String,personId:java.lang.String,repoId:java.lang.String,
                    taskId:java.lang.String, blackId:java.lang.String,faceId:java.lang.String,
                    confidence:java.lang.String,time:java.lang.String,alarmType:java.lang.String,
                    recieveTime:java.lang.String)

  def parseClass(arr : List[String]):Option[Any] = {
    try{
        Some(Warning(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8),arr(9)))
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
    val warnings = (p+",end").split(",") match {
      case arr:Array[String] if( arr.size == 11 ) => parseClass(arr.toList) match {
                                                                                    case Some(x) => x.asInstanceOf[Warning]
                                                                                    case _ => return List(PARSEERROR)
                                                                                  }
      case _ => return List(PARSEERROR)
    }

    // make sure hit the id
    val warning = scala.collection.mutable.ListBuffer[String]()
    warning += warnings.toString
    warning.toList
  }

}
