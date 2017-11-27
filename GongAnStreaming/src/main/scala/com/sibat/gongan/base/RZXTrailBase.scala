package com.sibat.gongan.base
/****
  wifi热点车载轨迹
***/
import com.sibat.gongan.imp.{Core,ESQueryTrait,CommonCoreTrait,IPropertiesTrait,StatusTrait}

object RZXTrailBase extends Core with ESQueryTrait with IPropertiesTrait with CommonCoreTrait{

  case class Trail(devicenum:java.lang.String,devicemac:java.lang.String,xpoint:java.lang.String,ypoint:java.lang.String,rate:java.lang.String,
                   hight:java.lang.String,activetime:java.lang.String,servicecode:java.lang.String,recieveTime:java.lang.String)

  def parseClass(arr : List[String]):Option[Any] = {
    try{
        Some(Trail(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8)))
        }
    catch{
            case ex:Throwable => None
        }
  }

  // def Alarm(p:String):Map[String,String] = {
  //   val hitmark = new EZJSONParser(
  //                 ESTermQuery(ESINDEX,ESTYPE,"cardid",p).toString
  //                   )
  //                     .getMap("hits")
  //                     .mark
  //
  //   // make sure hit the id
  //   if (hitmark.getMap("total").query == "0.0")
  //     return Map[String,String]("status" -> "NotFound!")
  //
  //   val jsonmark = hitmark
  //                     .getMap("hits")
  //                     .getList(0)
  //                     .getMap("_source")
  //                     .mark
  //
  //   Map[String,String]("key" -> jsonmark.getMap("cardid").query,
  //                       "context" -> jsonmark.getMap("col1").query,
  //                       "age" -> "test",
  //                       "status" -> "Hit"
  //                     )
  // }

}
