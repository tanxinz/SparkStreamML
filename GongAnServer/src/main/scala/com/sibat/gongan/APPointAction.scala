package com.sibat.gongan

import com.sibat.gongan.imp.HBaseActionTrait

class APPointAction extends HBaseActionTrait{

  case class APPoint(station:String,time:String,term:String)

  override def format(arrlist:List[List[Map[String,String]]]) = {
    val res = new scala.collection.mutable.ListBuffer[APPoint]()
    val temp = scala.collection.mutable.Map[String,Int]()
    for(arr <- arrlist){
      for(i <- arr){
          val row = i("row").split("#");
          val value = i("value").split(",")
          res += APPoint(value(0),row(1),value(1))
      }
    }
    res.toList
  }


  override def tablename() = "ap_point"
}
