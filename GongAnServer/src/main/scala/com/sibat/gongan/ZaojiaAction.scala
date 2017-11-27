package com.sibat.gongan

import com.sibat.gongan.imp.HBaseQueryTrait
import org.scalatra._
// JSON-related libraries
import org.json4s.{DefaultFormats, Formats}
// JSON handling support from Scalatra
import org.scalatra.json._
import org.elasticsearch.common.xcontent.XContentFactory._

class ZaojiaAction extends ScalatraServlet with JacksonJsonSupport with HBaseQueryTrait {

    protected implicit lazy val jsonFormats: Formats = DefaultFormats

  before() {
    contentType = formats("json")
  }

  get("/fuck") {
    response.setHeader("Access-Control-Allow-Origin","*")
    val test = getTimes()
    val szt = format(read("zaojia/szts"),"深圳通")
    val ty = format( read("zaojia/imsis"),"电子围栏")
    val rzx = format( read("zaojia/macs"),"WIFI热点")
    val ap = format( read("zaojia/macs"),"AP定位")
    val res = szt ++ ap ++ rzx ++ ty
    val ress = res.toList.sortWith(_ > _)
    val rm = (new java.util.Random()).nextInt(3)
    val ret = new scala.collection.mutable.ListBuffer[String]()
    for (i <- 0 to 100){
      ret += ress((new java.util.Random()).nextInt(399))
    }
    ret.toList
  }

  get("/txt/:type") {
    response.setHeader("Access-Control-Allow-Origin","*")
    val test = getTimes()
    params("type") match {
      case "szt" => format(read("zaojia/szts"),"深圳通")
      case "ty" => format( read("zaojia/imsis"),"电子围栏")
      case "rzx" => format( read("zaojia/macs"),"WIFI热点")
      case "ap" => format( read("zaojia/macs"),"AP定位")
      case "sensordoor" => {
        val res = new scala.collection.mutable.ListBuffer[String]()
        val gids = new scala.collection.mutable.ListBuffer[String]()
        val gid = scala.io.Source.fromFile("genids").getLines
        for(i <- gid) gids += i
        val rm = (new java.util.Random()).nextInt(100)
        val rs = (new java.util.Random()).nextInt(10)+1
        for (i <- rs to rm*rs by rs) {
          res += gids(i)
        }
        res.toList
      }
    }
  }

  def format(arr:List[String],typs:String) = {
    val res = new scala.collection.mutable.ListBuffer[String]()
      for(i <- 1 to 100){
        res += arr((new java.util.Random()).nextInt(60000))+","+getTimes()+","+typs
      }
    res
  }

  def read(path:String) ={
    val gids = new scala.collection.mutable.ListBuffer[String]()
    val gid = scala.io.Source.fromFile(path).getLines
    for(i <- gid) gids += i
    gids.toList
  }

  get("/fuckPhoto"){
    response.setHeader("Access-Control-Allow-Origin","*")
    val res = new scala.collection.mutable.ListBuffer[String]()
    val gids = new scala.collection.mutable.ListBuffer[String]()
    val gid = scala.io.Source.fromFile("genids").getLines
    for(i <- gid) gids += i
    val gsize = gids.size
    for (i <- 1 to 100) {
      res += gids((new java.util.Random()).nextInt(gsize -1))
    }
    res.toList
  }


  def getTimes() = {
    val timeformat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val now = new java.util.Date()
    timeformat.format(now)
  }

}
