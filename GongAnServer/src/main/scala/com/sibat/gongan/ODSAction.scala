package com.sibat.gongan

import scala.util.Try
import scala.util.parsing.json.JSON

import org.scalatra._
// JSON-related libraries
import org.json4s.{DefaultFormats, Formats}
// JSON handling support from Scalatra
import org.scalatra.json._
import org.elasticsearch.common.xcontent.XContentFactory._

import com.sibat.gongan.imp.HBaseActionTrait

class ODSAction extends HBaseActionTrait{

  case class OD(start:String,end:String,ods:Int)

  override def tablename() = "odcounter"

  override def format(arrlist:List[List[Map[String,String]]]) = {
    val res = new scala.collection.mutable.ListBuffer[OD]()
    val temp = scala.collection.mutable.Map[String,Int]()
    for(arr <- arrlist){
      for(i <- arr){
          val row = i("row").split("-");
          temp += (row(4)+"-"+row(5) -> i("value").toInt)
      }
    }
    for((k,v) <- temp){
      val L = k.split("-")
      res += OD(L(1),L(0),v)
    }
    res.toList
  }

  def getTimes() = {
		val timeformat = new java.text.SimpleDateFormat("yyyy-MM-dd-HH:mm:ss")
		val cal = java.util.Calendar.getInstance()
		lazy val now = new java.util.Date()
		cal.setTime(now)
		cal.add(java.util.Calendar.MINUTE,-60)
		(timeformat.format(now),timeformat.format(cal.getTime))
	}

  get("/regex/:regex") {
    val times = getTimes()
    params("regex") match {
      case "all" => {val res = HBaseScanByKey(tablename,times._2,times._1)
                    if (res!= null) Map("all" -> format(res))
                    else halt(500,"Not Found!!")
                  }
      case _ => { val temp = scala.collection.mutable.Map[String,List[OD]]()
                  val o = HBaseQueryNewest(tablename,times._2,times._1,"-"+params("regex")+"-")
                  if (o!= null) temp += ("d" -> format(o))
                  else halt(500,"Not Found!!")

                  val d = HBaseQueryNewest(tablename,times._2,times._1,"-"+params("regex")+"$")
                  if (d!= null) temp += ("o" -> format(d))
                  else halt(500,"Not Found!!")
                  temp
      }
    }

  }

  // get("/get/all") {
    // val res = HBaseScanByRegexFilter(tablename,)
    // if (res!= null) format(res)
    // else halt(404,"Not Found!!")
  // }

}
