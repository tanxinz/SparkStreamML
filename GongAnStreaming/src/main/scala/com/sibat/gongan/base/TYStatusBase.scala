package com.sibat.gongan.base
/****
  天彦电子围栏IMSI数据
***/
import com.sibat.gongan.imp.{Core,ESQueryTrait,CommonCoreTrait,IPropertiesTrait,StatusTrait}

object TYStatusBase extends Core with ESQueryTrait with IPropertiesTrait with CommonCoreTrait{

  case class Status(deviceId:java.lang.String,time:java.lang.String,status:java.lang.String,localtion:java.lang.String,recieveTime:java.lang.String)

  def parseClass(arr : List[String]):Option[Any] = {
    try{
        Some(Status(arr(0),arr(1),arr(2),arr(3),arr(4)))
        }
    catch{
            case ex:Throwable => None
        }
  }

//   def Alarm(p:String):Map[String,String] = {
//     val hitmark = new EZJSONParser(
//                   ESTermQuery(ESINDEX,ESTYPE,"cardid",p).toString
//                     )
//                       .getMap("hits")
//                       .mark
//
//     // make sure hit the id
//     if (hitmark.getMap("total").query == "0.0")
//       return Map[String,String]("status" -> "NotFound!")
//
//     val jsonmark = hitmark
//                       .getMap("hits")
//                       .getList(0)
//                       .getMap("_source")
//                       .mark
//
//     Map[String,String]("key" -> jsonmark.getMap("cardid").query,
//                         "context" -> jsonmark.getMap("col1").query,
//                         "age" -> "test",
//                         "status" -> "Hit"
//                       )
//   }

}
