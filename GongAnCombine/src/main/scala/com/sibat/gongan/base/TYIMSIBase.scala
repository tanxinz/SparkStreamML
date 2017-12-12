package com.sibat.gongan.base
/****
  天彦电子围栏IMSI数据
***/
//
import com.sibat.gongan.imp.Core
//
object TYIMSIBase extends Core {
//
  def getDF(sqlContext:SQLContext,date:String,datapath:String) = {
    val location = sqlContext.read.parquet("DeviceStation/rzx_feature")
    val df = sqlContext.read.parquet(datapath+"/ty_imsi/"+date).drop("station")
    df.join(location,Seq("deviceId"),"inner").withColumnRenamed("station","rzxstation")
  }
//   case class IMSI(deviceId:java.lang.String,time:java.lang.String,imsi:java.lang.String,imei:java.lang.String,location:java.lang.String,recieveTime:java.lang.String)
//
//   def parseClass(arr : List[String]):Option[Any] = {
//     try{
//       Some(IMSI(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5)))
//     }catch{
//       case ex:Throwable=> None
//     }
//   }
//
//   def Alarm(p:String):List[String] = {
//     //加上end，scala的split不会分割出最后一直为空的逗号
//     val imsi = (p+",end").split(",") match {
//       case arr:Array[String] if( arr.size == 7 ) => parseClass(arr.toList) match {
//                                                                                   case Some(imsi) => imsi.asInstanceOf[IMSI]
//                                                                                   case _ => return List(PARSEERROR)
//                                                                                   }
//       case _ => return List(PARSEERROR)
//     }
//     // make sure hit the id
//     val warning = scala.collection.mutable.ListBuffer[String]()
//     for ((target,record) <- Map("imsi" -> imsi.imsi,"imei" -> imsi.imei)){
//       queryFound(PERSONALINDEX,ESTYPE,target,record) match {
//         case Some(id) => warning += List("ty",target,record,id._1,imsi.time,imsi.deviceId,id._2).mkString(",")
//         case None =>
//       }
//     }
//     // if (warning.size != 0) {
//     //   return warning.toList
//     // }
//     //
//     // queryFound(IMSIINDEX,ESTYPE,"imsi",imsi.imsi) match {
//     //   case Some(id) => warning += (List(IMSIINDEX,ESTYPE,"imsi",id).mkString(",")+","+imsi.toString)
//     //   case None =>
//     // }
//     //
//     // queryFound(IMEIINDEX,ESTYPE,"imei",imsi.imei) match {
//     //   case Some(id) => warning += (List(IMEIINDEX,ESTYPE,"imei",id).mkString(",")+","+imsi.toString)
//     //   case None =>
//     // }
//     warning.toList
//   }
//
//
}
