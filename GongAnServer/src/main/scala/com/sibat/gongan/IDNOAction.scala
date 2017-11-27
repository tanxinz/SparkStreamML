// package com.sibat.gongan
//
// import scala.util.Try
// import scala.util.parsing.json.JSON
//
// import org.scalatra._
// // JSON-related libraries
// import org.json4s.{DefaultFormats, Formats}
// // JSON handling support from Scalatra
// import org.scalatra.json._
// import org.elasticsearch.common.xcontent.XContentFactory._
//
// import com.sibat.gongan.imp.{ESQueryTrait,IPropertiesTrait}
//
// class IDNOAction extends ScalatraServlet with JacksonJsonSupport with ESQueryTrait with IPropertiesTrait{
//
//   // Sets up automatic case class to JSON output serialization, required by
//   // the JValueResult trait.
//   protected implicit lazy val jsonFormats: Formats = DefaultFormats
//
//   case class IDNO(idno:String,important:String)//,imsi:String,imei:String,szt:String,important:String)
//
//   // Before every action runs, set the content type to be in JSON format.
//   before() {
//     contentType = formats("json")
//   }
//
//   get("/get/:id") {
//     val res = ESGet(MACINDEX,ESTYPE,params("id")).getSourceAsString()
//     if (res!= null) res
//     else halt(404,"Not Found!!")
//   }
//
//   post("/add"){
//     val ok = Try {
//       val mac = JSON.parseFull(request.body) match {
//         case Some(x) => x.asInstanceOf[Map[String,String]]
//         case _ => halt(500,"parse error!!")
//       }
//         val body = jsonBuilder()
//             .startObject()
//               .field("idno", mac("idno"))
//               .field("important", mac("important"))
//             .endObject()
//         ESUpsert(IDNOINDEX,ESTYPE,mac("idno"),body)
//       }.isSuccess
//     if(ok) "ok!"
//     else halt(500,"parse error!!")
//   }
//
// }

package com.sibat.gongan

import com.sibat.gongan.imp.HBaseActionTrait

class IDNOAction extends HBaseActionTrait{

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


  override def tablename() = "idno"
}
