package com.sibat.gongan

import com.sibat.gongan.imp.HBaseActionTrait

class RZXFeatureAction extends HBaseActionTrait{

  case class Feature(station:String,time:String,term:String,gps:Map[String,String])

  override def format(arrlist:List[List[Map[String,String]]]) = {
    val res = new scala.collection.mutable.ListBuffer[Feature]()
    val temp = scala.collection.mutable.Map[String,Int]()
    for(arr <- arrlist){
      for(i <- arr){
          val row = i("row").split("#");
          val value = i("value").split(",")
          res += Feature(value(0),row(1),value(0),Map("x"->value(2),"y"->value(1)))
      }
    }
    res.toList
  }

  get("/scanNew/:id/:start/:end") {
    val start = params("id")+"#"+params("start")
    val end = params("id")+"#"+params("end")
    val res = HBaseScanByKey(tablename,start,end)
    if (res!= null) format(res)
    else halt(404,"Not Found!!")
  }


  override def tablename() = "rzx_feature"
}
