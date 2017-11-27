package com.sibat.gongan

import com.sibat.gongan.imp.HBaseActionTrait

class SZTAction extends HBaseActionTrait{

  case class SZT(station:String,time:String,term:String,datatype:String)

  override def tablename() = "szt"

  def formatcount(arrlist:List[List[Map[String,String]]]) = {
    var res = 0
    for(arr <- arrlist){
      for(i <- arr){
          res = i("value").toInt
      }
    }
    res
  }

  override def format(arrlist:List[List[Map[String,String]]]) = {
    val res = new scala.collection.mutable.ListBuffer[SZT]()
    val temp = scala.collection.mutable.Map[String,Int]()
    for(arr <- arrlist){
      for(i <- arr){
          val row = i("row").split("-")
          val value = i("value").split(",")
          res += SZT(value(0),row.tail.mkString("-"),value(1),"szt")
      }
    }
    res.toList
  }

  def getTimes(diff:Int = 0) = {
    val timeformat = new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
    val cal = java.util.Calendar.getInstance()
    val now = new java.util.Date()
    cal.setTime(now)
    cal.add(java.util.Calendar.DAY_OF_MONTH,diff)
    val time1 = cal.getTime
    cal.add(java.util.Calendar.MINUTE,-16)
    val time2 = cal.getTime
    (timeformat.format(time1),timeformat.format(time2))
  }

  get("/get/count") {
    val tablename = "sztcountall"
    val times = getTimes()
    val now = HBaseScanByKey(tablename,times._2,times._1)
    val dodtime = getTimes(-1)
    val dod = HBaseScanByKey(tablename,dodtime._2,dodtime._1)
    val wowtime = getTimes(-7)
    val wow = HBaseScanByKey(tablename,wowtime._2,wowtime._1)
    if (now!= null) Map("count" -> formatcount(now),"tb" -> formatcount(dod),"hb" -> formatcount(wow))
    else halt(500,"Not Found!!")
    }

  get("/station/warning") {
    val tablename = "szt_predict"
    Map("data" -> List("1268008000","1260013000"))
  }

    case class Trail(station:String,time:String,term:String)

    get("/scanNew/:id/:start/:end") {
      val start = params("id")+"#"+params("start")
      val end = params("id")+"#"+params("end")
      val res = HBaseScanByKey(tablename,start,end)
      if (res!= null) trailformat(res)
      else halt(404,"Not Found!!")
    }

    def trailformat(arrlist:List[List[Map[String,String]]]) = {
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

}
