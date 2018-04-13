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

  def filter(astart:String,aend:String,bstart:String,bend:String) = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val d1start = format.parse(astart)
    val d1end = format.parse(aend)
    val d2start = format.parse(bstart)
    val d2end = format.parse(bend)
    (! (d1start.after(d2end) || d2start.after(d1end)) )
  }

/**
  LAST_TIME_IN_METRO_STATION：second
**/
  def addTime(format:java.text.SimpleDateFormat)(f:(Long,Long)=>Long,a:String) = {
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
