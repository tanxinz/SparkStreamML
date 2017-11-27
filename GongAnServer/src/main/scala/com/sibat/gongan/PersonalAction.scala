package com.sibat.gongan

import scala.util.Try
import scala.util.parsing.json.JSON

import org.scalatra._
// JSON-related libraries
import org.json4s.{DefaultFormats, Formats}
// JSON handling support from Scalatra
import org.scalatra.json._
import org.elasticsearch.common.xcontent.XContentFactory._

import com.sibat.gongan.imp.{HBaseQueryTrait,ESQueryTrait,IPropertiesTrait}
import com.sibat.gongan.util.PersonalColumns
import com.sibat.gongan.util.EZJSONParser

class PersonalAction extends ScalatraServlet with JacksonJsonSupport with ESQueryTrait with IPropertiesTrait with HBaseQueryTrait{

  // Sets up automatic case class to JSON output serialization, required by
  // the JValueResult trait.
  protected implicit lazy val jsonFormats: Formats = DefaultFormats

  case class Personal(id:String,genid:String,mac:String)//,imsi:String,imei:String,szt:String,important:String)

  def defaultGet(map:Map[String,String])(key:String):String = {
    map.get(key) match {
      case Some(a) => a.toString
      case _ => ""
    }
  }

  // Before every action runs, set the content type to be in JSON format.
  before() {
    contentType = formats("json")
  }

  get("/get/:id") {
    val res = ESGet(PERSONALINDEX,ESTYPE,params("id")).getSourceAsString()
    if (res!= null) res
    else halt(404,"Not Found!!")
  }

  post("/add"){
    val ok = Try {
      // println(params.getOrElse("mac",'1'))
        // val personal = parsedBody.extract[Personal]
      val personals = JSON.parseFull(request.body) match {
        case Some(x) => x.asInstanceOf[Map[String,String]]
        case _ => halt(500,"parse error!!")
      }
      val personal = defaultGet(personals)_
        val body = jsonBuilder().startObject()
        for(i <- PersonalColumns.Columns){
          body.field(i,personal(i))
        }
        body.endObject
        ESUpsert(PERSONALINDEX,ESTYPE,personal("idno"),body)
      }.isSuccess
    if(ok) "ok!"
    else halt(500,"parse error!!")
  }

  get("/delete/:id") {
    val ok = Try{
      ESDelete(PERSONALINDEX,ESTYPE,params("id"))
    }.isSuccess
    if(ok) "ok!"
    else halt(500,"parse error!!")
  }

  get("/get/all/start") {
    val res = ESScrollStart(PERSONALINDEX,ESTYPE,"200".toInt)
    Map("size" -> res.getHits().getTotalHits(),"id" -> res.getScrollId())
  }

  get("/get/all/id/:id") {
    val response = ESScroll(params("id"))
    val hits = response.getHits().getHits()
    val res = scala.collection.mutable.ListBuffer[Any]()
    for (hit <- hits) {
         res += JSON.parseFull(hit.getSourceAsString())
    }
    Map("id" -> response.getScrollId(),"data" -> res)
  }

  // get("/get/all/:size/:from"){
  //   val hits = ESMatchAll(PERSONALINDEX,ESTYPE,params("size").toInt,params("from").toInt).getHits().getHits()
  //   val res = scala.collection.mutable.ListBuffer[Any]()
  //   for (hit <- hits) {
  //      res += hit.getSourceAsString();
  //   }
  //   if (res!= null) res.toList
  //   else halt(404,"Not Found!!")
  // }

  get("/query/:index/:doc/:col/:tar"){
    val res = ESTermQuery(params("index"),params("doc"),params("col"),params("tar")).getHits().getHits()(0).getSourceAsString()
    if (res!= null) res
    else halt(404,"Not Found!!")
  }


  def formatRelation(arrlist:List[List[Map[String,String]]],firstsecond:Boolean) = {
      val res = new scala.collection.mutable.ListBuffer[Map[String,String]]()
      for(arr <- arrlist){
        for(i <- arr){
            val row = i("row").split("#")
            val value = i("value").split(",")
            if (firstsecond) {
              res += Map("id" -> row(1),"type" -> value(0),"alive" -> value(1),"freq" -> value(2))
            } else {
              res += Map("id" -> row(0),"type" -> value(0),"alive" -> value(1),"freq" -> value(2))
            }
        }
      }
      res
  }

  get("/query/together/:id"){
    val res1 = HBaseScanByRegexFilter("personal_together","^"+params("id")+"#")
    // val res2 = HBaseScanByRegexFilter("personal_together","#"+params("id")+"#")
    // val res = formatRelation(res1,true) ++ formatRelation(res2,false)
    // res.toList
    formatRelation(res1,true).toList
  }

  case class Relation(value:String,datatype:String)

  get("/get/topology/:id"){
    // import com.sibat.gongan.util.EZJSONParser
    val res = ESGet(PERSONALINDEX,ESTYPE,params("id")).getSourceAsString()
    if(res == null) halt(404,"Not Found!!")
    else {
      val js = JSON.parseFull(res).get.asInstanceOf[Map[String,Any]]
      import scala.collection.mutable.Map
      val id  = Map[String,Any]()
      id += ("id" -> Map("name" -> "id","value"->params("id"),"target" -> List("mac"+js("mac"),
                                            "imsi"+js("imsi"),"idno"+js("idno"),"szt"+js("szt"))))
      val szt = ("szt"+js("szt") -> Map("name" -> "szt","value" -> js("szt"),"target" -> List()))
      val mac = ("mac"+js("mac") -> Map("name" -> "mac","value" -> js("mac"),"target" -> List()))
      val imsi = ("imsi"+js("imsi") -> Map("name" -> "imsi","value" -> js("imsi"),"target" -> List()))
      val idno = ("idno"+js("idno") -> Map("name" -> "idno" ,"value" -> js("idno"),"target" -> List()))
      id+=szt
      id += mac
      id+= imsi
      id+= idno
      id
    }

    // val nmark = new EZJSONParser(res).getMap("hits").mark
      // make sure hit the id
    // if (nmark.getMap("total").query == "0.0") halt(404,"Not Found!!")
    // else {

  // }
    // // Map("id"-> "123"relations" -> Map("id" -> List(Relation("4","mac"),Relation("5","imsi"),
    //                                   // Relation("test","name")),"4mac"->List(Relation("130","phone")),
    //                                   // "130phone" -> List(Relation("888","qq"))))
    //   import scala.collection.mutable.Map
    //   // val szt = ("szt123456" -> Map("name" -> "szt","value" -> "123456","target" -> List("mac123","imsi234567","mac234")))
    //   // val mac1 = ("mac123" -> Map("name" -> "mac","value" -> "123","target" -> List("szt456","imsi123")))
    //   // val mac2 = ("mac234" -> Map("name" -> "mac","value" -> "234","target" -> List()))
    //   // val imsi = ("imsi123" -> Map("name" -> "imsi","value" -> "123","target" -> List()))
    //   // val idno = ("idno323" -> Map("name" -> "idno" ,"value" -> "323","target" -> List()))
    //   // val id  = Map[String,Any]()
    //   id += ("id" -> Map("name" -> "id","value"->"323","target" -> List("mac123","imsi123","idno323","szt123456")))
    //   // id+=szt
    //   // id += mac1
    //   // id += mac2
    //   // id+= imsi
    //   // id+= idno
    //   id
    }

}
