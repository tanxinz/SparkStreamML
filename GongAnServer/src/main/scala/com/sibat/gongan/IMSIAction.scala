package com.sibat.gongan

import com.sibat.gongan.imp.HBaseActionTrait

class IMSIAction extends HBaseActionTrait{

  case class Trail(station:String,time:String,term:String)

  override def format(arrlist:List[List[Map[String,String]]]) = {
    val res = new scala.collection.mutable.ListBuffer[Trail]()
    val temp = scala.collection.mutable.Map[String,Int]()
    for(arr <- arrlist){
      for(i <- arr){
          val row = i("row").split("#");
          val value = i("value").split(",")
          res += Trail(value(0),row(1),value(1))
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


  override def tablename() = "imsi"
}
