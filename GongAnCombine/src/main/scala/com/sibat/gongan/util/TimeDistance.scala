package com.sibat.gongan.util

import com.sibat.gongan.imp.IPropertiesTrait

object TimeDistance extends IPropertiesTrait{

  def filter(a:String,b:String) = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val d1 = format.parse(a)
    val d2 = format.parse(b)
    if(scala.math.abs(d1.getTime - d2.getTime) > CLOSETIME.toInt * 1000) false else true
  }

}
