package com.sibat.gongan.util

import com.sibat.gongan.imp.IPropertiesTrait

object TimeDistance extends IPropertiesTrait{

  /**
    CLOSETIME 秒
  **/
  def filter(a:String,b:String) = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val d1 = format.parse(a)
    val d2 = format.parse(b)
    scala.math.abs(d1.getTime - d2.getTime) <= CLOSETIME.toInt * 1000
  }

/**
  LAST_TIME_IN_METRO_STATION：second
**/
  def addTime(f:(Long,Long)=>Long,a:String) = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val d = format.parse(a)
    format.format(
      new java.util.Date(
        f.apply(d.getTime , LAST_TIME_IN_METRO_STATION.toInt * 1000)
      )
    )
  }

  /**
    LAST_TIME_IN_METRO_STATION：second
  **/
    def addTime(f:(Long,Long)=>Long,a:String,span:Int) = {
      val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val d = format.parse(a)
      format.format(
        new java.util.Date(
          f.apply(d.getTime , span * 1000)
        )
      )
    }

}
