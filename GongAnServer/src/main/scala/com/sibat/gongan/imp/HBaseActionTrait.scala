package com.sibat.gongan.imp

import scala.util.Try
import scala.util.parsing.json.JSON

import org.scalatra._
// JSON-related libraries
import org.json4s.{DefaultFormats, Formats}
// JSON handling support from Scalatra
import org.scalatra.json._
import org.elasticsearch.common.xcontent.XContentFactory._


trait HBaseActionTrait extends ScalatraServlet with JacksonJsonSupport with HBaseQueryTrait with IPropertiesTrait{
  // Sets up automatic case class to JSON output serialization, required by
  // the JValueResult trait.
  protected implicit lazy val jsonFormats: Formats = DefaultFormats

  def tablename() = ""

  def format(res:List[List[Map[String,String]]]):Any = res

  // Before every action runs, set the content type to be in JSON format.
  before() {
    contentType = formats("json")
  }

  get("/get/:id") {
    val res = HBaseQueryByKey(tablename,params("id"))
    if (res!= null) res
    else halt(404,"Not Found!!")
  }

  get("/scan/:start/:stop") {
    val res = HBaseScanByKey(tablename,params("start"),params("stop"))
    if (res!= null) res
    else halt(404,"Not Found!!")
  }

  get("/scanNew/:id/:start/:end") {
    val start = params("id")+"-"+params("start")
    val end = params("id")+"-"+params("end")
    val res = HBaseScanByKey(tablename,start,end)
    if (res!= null) format(res)
    else halt(404,"Not Found!!")
  }

  get("/prefix/:prefix") {
    val res = HBaseScanByPrefixFilter(tablename,params("prefix"))
    if (res!= null) res
    else halt(404,"Not Found!!")
  }

  get("/regex/:regex") {
    val res = HBaseScanByRegexFilter(tablename,params("regex"))
    if (res!= null) res
    else halt(404,"Not Found!!")
  }
}
