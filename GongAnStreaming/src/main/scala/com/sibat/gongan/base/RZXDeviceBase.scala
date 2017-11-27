package com.sibat.gongan.base
/****
  wifi热点设备信息
***/
import com.sibat.gongan.imp.{Core,ESQueryTrait,CommonCoreTrait,IPropertiesTrait,StatusTrait}

object RZXDeviceBase extends Core with ESQueryTrait with IPropertiesTrait with CommonCoreTrait{

  case class Device(equipmentnum:java.lang.String,equipmentname:java.lang.String,mac:java.lang.String,servicecode:java.lang.String,provincecode:java.lang.String,
                    citycode:java.lang.String,areacode:java.lang.String,equipmenttype:java.lang.String,longtitude:java.lang.String,latitude:java.lang.String,
                    subwaystation:java.lang.String,subwayvehicleinfo:java.lang.String,subwaylineinfo:java.lang.String,
                    subwaycompartmentnum:java.lang.String,carcode:java.lang.String,uploadtimeinterval:java.lang.String,collectionradius:java.lang.String
                    ,createtime:java.lang.String,recieveTime:java.lang.String)

  def parseClass(arr : List[String]):Option[Any] = {
    try{
        Some(Device(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8),
                    arr(9),arr(10),arr(11),arr(12),arr(13),arr(14),arr(15),arr(16),arr(17),arr(18)))
        }
    catch{
            case ex:Throwable => None
        }
  }

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
