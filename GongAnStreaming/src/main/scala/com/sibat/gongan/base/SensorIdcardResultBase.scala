package com.sibat.gongan.base
/****
  公安一所感知门身份证
***/
import com.sibat.gongan.imp.{Core,ESQueryTrait,CommonCoreTrait,IPropertiesTrait,StatusTrait}


object SensorIdcardResultBase extends Core with ESQueryTrait with IPropertiesTrait with CommonCoreTrait{

  case class IdcardResult(mac:java.lang.String,genId:java.lang.String,timeStamp:java.lang.String,
                          idno:java.lang.String,hcjg:java.lang.String,bjxx:java.lang.String,recieveTime:java.lang.String)

  def parseClass(arr : List[String]):Option[Any] = {
    try{
        Some(IdcardResult(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6)))
        }
    catch{
            case ex:Throwable=> None
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
