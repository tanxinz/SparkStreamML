package com.sibat.gongan.base
/****
  公安一所感知门照片
***/
import com.sibat.gongan.imp.{Core,ESQueryTrait,CommonCoreTrait,IPropertiesTrait,StatusTrait}
import com.sibat.gongan.util.EZJSONParser

object SensorFaceBase extends Core with ESQueryTrait with IPropertiesTrait with CommonCoreTrait with StatusTrait{

  case class Face(mac:java.lang.String,genid:java.lang.String,timeStamp:java.lang.String,captureface:java.lang.String,
                  faceid:java.lang.String,fscore:java.lang.String,info1:java.lang.String,
                  face1:java.lang.String,info2:java.lang.String,
                  face2:java.lang.String,
                  info3:java.lang.String,face3:java.lang.String,
                  info4:java.lang.String,face4:java.lang.String,recieveTime:java.lang.String)

  def parseClass(arr : List[String]):Option[Any] = {
    try{
        Some(Face(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8),arr(9),arr(10),arr(11),arr(12),arr(13),arr(14)))
        }
    catch{
            case ex:Throwable => None
        }
  }
//
//
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
