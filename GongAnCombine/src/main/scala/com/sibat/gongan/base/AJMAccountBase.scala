package com.sibat.gongan.base

// import com.sibat.gongan.util.EZJSONParser
// import com.sibat.gongan.imp.{Core,ESQueryTrait,CommonCoreTrait,IPropertiesTrait,StatusTrait}
//
//
// object AJMAccountBase extends Core with ESQueryTrait with IPropertiesTrait with CommonCoreTrait{
//
//   case class Account(device_mac:java.lang.String,app_code:java.lang.String,internal_id:java.lang.String,user_account:java.lang.String,
//                   nick_name:java.lang.String,phone:java.lang.String,imsi:java.lang.String,imei:java.lang.String,cap_time:java.lang.String,
//                   ap_mac:java.lang.String,coordinate:java.lang.String,dev_addr:java.lang.String,recieveTime:java.lang.String)
//
//   def parseClass(arr : List[String]):Option[Any] = {
//     try{
//         Some(Account(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8),arr(9),arr(10),arr(11),arr(12)))
//         }
//     catch{
//             case ex:Throwable => None
//         }
//   }
//
//   /****
//     return List
//     esindex,estype,escol,esid,
//   ****/
//   def Alarm(p:String):List[String] = {
//     //加上end，scala的split不会分割出最后一直为空的逗号
//     val account = (p+",end").split(",") match {
//       case arr:Array[String] if( arr.size == 14 ) => parseClass(arr.toList) match {
//                                                                                     case Some(x) => x.asInstanceOf[Account]
//                                                                                     case _ => return List(PARSEERROR)
//                                                                                   }
//       case _ => return List(PARSEERROR)
//     }
//
//     // make sure hit the id
//     val warning = scala.collection.mutable.ListBuffer[String]()
//     queryFound(PERSONALINDEX,ESTYPE,"mac",account.device_mac) match {
//       case Some(id) => warning += List("ajm","mac",account.device_mac,id._1,account.cap_time,account.dev_addr,id._2).mkString(",")
//       case None =>
//     }
//     // if (warning.size != 0) {
//     //   return warning.toList
//     // }
//     //
//     // queryFound(MACINDEX,ESTYPE,"mac",account.device_mac) match {
//     //   case Some(id) => warning += (List(MACINDEX,ESTYPE,"mac",id).mkString(",")+","+account.toString)
//     //   case None =>
//     // }
//     warning.toList
//   }
//
// }
