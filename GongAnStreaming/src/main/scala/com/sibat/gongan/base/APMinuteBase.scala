package com.sibat.gongan.base

import com.sibat.gongan.util.EZJSONParser
import com.sibat.gongan.imp.{Core,ESQueryTrait,CommonCoreTrait,IPropertiesTrait,StatusTrait}

/**
  存储AP的室内热力数据，一个站一个，用来做预测，不需要预警
**/
object APMinuteBase extends Core with ESQueryTrait with IPropertiesTrait with CommonCoreTrait{

  case class Minute(stime:java.lang.String,bid:java.lang.String,fid:java.lang.String,aid:java.lang.String,stay:java.lang.String,
                    visits:java.lang.String,avgstay:java.lang.String,recieveTime:java.lang.String)

  def parseClass(arr : List[String]):Option[Any] = {
    try{
        Some(Minute(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7)))
        }
    catch{
            case ex:Throwable => None
        }
  }

}
