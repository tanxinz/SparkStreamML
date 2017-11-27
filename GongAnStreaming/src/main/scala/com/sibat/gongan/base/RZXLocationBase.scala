package com.sibat.gongan.base
/****
  wifi热点场所资料
***/
import com.sibat.gongan.imp.{Core,ESQueryTrait,CommonCoreTrait,IPropertiesTrait,StatusTrait}

object RZXLocationBase extends Core with ESQueryTrait with IPropertiesTrait with CommonCoreTrait{

  case class Location(servicecode:java.lang.String,servicename:java.lang.String,status:java.lang.String,servicetype:java.lang.String,
                      provincecode:java.lang.String,citycode:java.lang.String,areacode:java.lang.String,xpoint:java.lang.String,ypoint:java.lang.String
                      ,recieveTime:java.lang.String)

  def parseClass(arr : List[String]):Option[Any] = {
    try{
        Some(Location(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8),arr(9)))
        }
    catch{
            case ex:Throwable => None
        }
  }

  // def Alarm(p:String):List[String] = {
  //   //加上end，scala的split不会分割出最后一直为空的逗号
  //   val location = (p+",end").split(",") match {
  //     case arr:Array[String] if( arr.size == 5 ) => parseClass(arr.toList) match {
  //                                                                               case Some(location) => location.asInstanceOf[Location]
  //                                                                               case _ => return List(PARSEERROR)
  //                                                                             }
  //     case _ => return List(PARSEERROR)
  //   }
  //   // make sure hit the id
  //   val warning = scala.collection.mutable.ListBuffer[String]()
  //   queryFound(PERSONALINDEX,ESTYPE,"mac",mac.mac) match {
  //     case Some(id) => warning += (List(PERSONALINDEX,ESTYPE,"mac",id).mkString(",")+","+mac.toString)
  //     case None =>
  //   }
  //   if (warning.size != 0) {
  //     return warning.toList
  //   }
  //
  //   queryFound(MACINDEX,ESTYPE,"mac",mac.mac) match {
  //     case Some(id) => warning += (List(MACINDEX,ESTYPE,"mac",id).mkString(",")+","+mac.toString)
  //     case None =>
  //   }
  //   warning.toList
  // }

}
