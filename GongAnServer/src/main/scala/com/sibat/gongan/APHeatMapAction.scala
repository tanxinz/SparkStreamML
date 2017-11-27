package com.sibat.gongan

import com.sibat.gongan.imp.HBaseActionTrait

class APHeatMapAction extends HBaseActionTrait{

  case class APHeatMap(time:String,visits:Int)

  override def format(arrlist:List[List[Map[String,String]]]) = {
    val res = new scala.collection.mutable.ListBuffer[APHeatMap]()
    val temp = scala.collection.mutable.Map[String,Int]()
    for(arr <- arrlist){
      for(i <- arr){
          val row = i("row").split("#");
          res += APHeatMap(row(1),(i("value").split("\\."))(0).toInt )
      }
    }
    val half = res.size
    for(i <- 1 until half){
      val vis = ((res(i-1).visits + res(i).visits) / 2).toInt
      val tim = addMin(res(i-1).time)
      res += APHeatMap(tim,vis)
    }
    res.toList.sortWith(_.time < _.time)
  }

  def addMin(times:String) = {
    val timeformat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val cal = java.util.Calendar.getInstance()
    cal.setTime(timeformat.parse(times))
    cal.add(java.util.Calendar.MINUTE,1)
    timeformat.format(cal.getTime)
  }

  override def tablename() = "apheatmap_predict"

  get("/predict/:times/:id"){
    val times = getTimes(params("times").toInt)
    val res = HBaseScanByPrefixFilter(tablename,params("id"),times._1,times._2)
    if (res!= null) format(rs)
    else halt(404,"Not Found!!")
  }

  def getTimes(min:Int) = {
    val timeformat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val cal = java.util.Calendar.getInstance()
    val now = new java.util.Date()
    cal.setTime(now)
    val time1 = cal.getTime
    cal.add(java.util.Calendar.MINUTE,min)
    val time2 = cal.getTime
    (timeformat.format(time1),timeformat.format(time2))
  }


}
