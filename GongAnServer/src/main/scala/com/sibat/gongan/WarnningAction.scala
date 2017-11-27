package com.sibat.gongan

import scala.util.Try
import com.sibat.gongan.imp.HBaseActionTrait

class WarnningAction extends HBaseActionTrait{

  // 厂商名，数据名（szt/mac/idno），数据id，id唯一标识（指的是es的id），设备编号，预警级别
  case class Warnning(stype:String,datatype:String,dataid:String,id:String,time:String,deviceId:String,important:String)

  override def format(arrlist:List[List[Map[String,String]]]) = {
    val res = new scala.collection.mutable.ListBuffer[Warnning]()
    for(arr <- arrlist){
      for(i <- arr){
          val row = i("row").split("#");
          val value = i("value").split(",")
          res += Warnning(value(0),row(1),row(2),row(4),row(0),row(3),value(1))
      }
    }
    res.toList
  }

  get("/station") {
    val times = getTimes()
    val res = HBaseScanByKey(tablename,times._2,times._1)
    if (res!= null) {
      import scala.collection.mutable.Map
      val rr = Map[String,Map[String,Int]]()
      for(i <- format(res)){
        val station = queryStation(i.stype,i.datatype,i.deviceId)
        i.important match {
          case "12" => {
            if(rr.contains("focus")){
              if (rr("focus").contains(station)){
                rr("focus") += (station -> (rr("focus")(station)+1))
              }
              else {
                rr("focus") += (station -> 1)
              }
            }
            else {
              rr += ("focus" -> Map(station -> 1))
            }
          }
          case "13" => {
            if(rr.contains("focus")){
              if (rr("focus").contains(station)){
                rr("focus") += (station -> (rr("focus")(station)+1))
              }
              else {
                rr("focus") += (station -> 1)
              }
            }
          else {
            rr += ("focus" -> Map(station -> 1))
          }
        }
        case "10" => {
            if(rr.contains("common")){
              if (rr("common").contains(station)){
                rr("common") += (station -> (rr("common")(station)+1))
              }
              else {
                rr("common") += (station -> 1)
              }
            }
            else {
              rr += ("common" -> Map(station -> 1))
            }
          }
          case "11" => {
              if(rr.contains("common")){
                if (rr("common").contains(station)){
                  rr("common") += (station -> (rr("common")(station)+1))
                }
                else {
                  rr("common") += (station -> 1)
                }
              }
              else {
                rr += ("common" -> Map(station -> 1))
              }
            }
          case _ =>
        }
      }
      jsonformat(rr)
    }
    else halt(404,"Not Found!!")
  }

  get("/stations/top5") {
    val res = HBaseScanLimit("warnning_stations",1)
    val value = res(0)(0)("value").split(",")
    if(value(0).size != 0){
      val maps = scala.collection.mutable.ListBuffer[Map[String,String]]()
      for(i <- value){
        val temp = i.split(":")
        maps += Map("name" -> temp(0),"value" -> temp(1))
      }
      maps.toList
    }
    else {
      List()
    }
  }


  def jsonformat(rr:scala.collection.mutable.Map[String,scala.collection.mutable.Map[String,Int]]) = {
    import scala.collection.mutable.ListBuffer
    import scala.collection.mutable.Map
    val res = Map[String,List[Map[String,String]]]()
    for(i <- List("focus","common")){
      Try{
        val ls = ListBuffer[Map[String,String]]()
        for ((k,v) <- rr(i) ){
          ls += Map("station" -> k,"value" -> v.toString)
        }
        res += (i -> ls.toList)
      }
    }
    res
  }


  /**
    目前的站点预警情况
  **/
  get("/get/now") {
    val times = getTimes()
    val res = HBaseScanByKey(tablename,times._2,times._1)
    if (res!= null) {
      nowjsonformat(res)
    }
    else halt(404,"Not Found!!")
  }

val sourcetypes = Map("rzx"->"WIFI热点","ap"->"AP定位","sensordoor" -> "感知门","ty"->"电子围栏","szt" -> "深圳通","ifass" -> "人脸识别")
 case class NowJson(source:String,datatype:String,dataid:String,id:String,time:String,deviceId:String,stype:String,station:String,photoPath:String="")
 def nowjsonformat(arrlist:List[List[Map[String,String]]]) = {
    val res = new scala.collection.mutable.ListBuffer[NowJson]()
    for(arr <- arrlist){
      for(i <- arr){
          val row = i("row").split("#");
          val value = i("value").split(",")
          value match {
            case Array(_,_) => res += NowJson(sourcetypes(value(0)),row(1),row(2),row(4),row(0),row(3),value(1),queryStation(value(0),row(1),row(3)))
            case _ => res += NowJson(sourcetypes(value(0)),row(1),row(2),row(4),row(0),row(3),value(1),queryStation(value(0),row(1),row(3)),value(2))
          }
      }
    }
    res.toList
  }

  get("/queryPersonal/:id") {
    val times = getDays(-30)
    val res = HBaseQueryNewest(tablename,times._2,times._1,params("id")+"$")
    if (res!= null){
      val ws = nowjsonformat(res)
      val ress = scala.collection.mutable.Map[String,Any]()
      val map = scala.collection.mutable.Map[String,Int]()
      for (c <- ws) {
        if(map.contains(c.station)){
          map += (c.station -> (map(c.station) + 1))
        }
        else {
          map += (c.station -> 1)
        }
      }
      val maps = scala.collection.mutable.ListBuffer[Map[String,String]]()
      for((k,v) <- map){
        maps += Map("name" -> k,"value" -> v.toString)
      }
      ress += ("stations" -> maps.toList)
      ress += ("recorder" -> ws)
      ress += ("freq" -> ws.size)
      ress
    }
    else halt(404,"Not Found!!")
  }

  def getDays(diff:Int = 0) = {
    val timeformat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val cal = java.util.Calendar.getInstance()
    val now = new java.util.Date()
    cal.setTime(now)
    val time1 = cal.getTime
    cal.add(java.util.Calendar.DAY_OF_MONTH,diff)
    val time2 = cal.getTime
    (timeformat.format(time1),timeformat.format(time2))
  }



  def getTimes(diff:Int = 0) = {
    val timeformat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val cal = java.util.Calendar.getInstance()
    val now = new java.util.Date()
    cal.setTime(now)
    cal.add(java.util.Calendar.DAY_OF_MONTH,diff)
    val time1 = cal.getTime
    cal.add(java.util.Calendar.MINUTE,-30)
    val time2 = cal.getTime
    (timeformat.format(time1),timeformat.format(time2))
  }


  /***
  根据数据源，数据类型，数据id来查询站点
  ***/
  def queryStation(stype:String,datatype:String,deviceid:String) = {
    deviceid
  }

  override def tablename() = "warnning"
}
