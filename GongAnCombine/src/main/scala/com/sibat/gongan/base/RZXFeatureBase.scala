package com.sibat.gongan.base
import com.sibat.gongan.imp.Core
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import com.sibat.gongan.util.TimeDistance
import com.sibat.gongan.base._


/****
  wifi热点特征日志
  属性名	字段长度要求	约束条件/说明
MAC	VARCHAR(17)	手机MAC地址
TYPE	INT(4)	数据类型,1 ap,2 mac,3 crc,9其他
START_TIME	LONG(20)	发现时间, 北京时间1970年1月1日08:0:0开始到结束时间的绝对秒数
END_TIME	LONG(20)	离开时间, 北京时间1970年1月1日08:0:0开始到结束时间的绝对秒数
POWER	VARCHAR(8)	信号强度
BSSID	VARCHAR(17)	连接方MAC，大写，短横杠，下同
ESSID	VARCHAR(256)	当前连接的AP的ssid
HISTORY_ESSID	VARCHAR(1024)	历史连接过的SSID,多个逗号分隔
MODEL	VARCHAR(128)	手机型号
OS_VERSION	VARCHAR(50)	系统版本
IMEI	VARCHAR(20)	IMEI
IMSI	VARCHAR(20)	IMSI
STATION	VARCHAR(20)	基站
XPOINT	VARCHAR(30)	数据捕获地图经度(只传输采集到的，当为空时，上层要做关联)
YPOINT	VARCHAR(30)	数据捕获地图纬度(只传输采集到的，当为空时，上层要做关联)
PHONE	VARCHAR(20)	手机号
DEVMAC	VARCHAR(17)	采集设备MAC地址，大写，短横杠
DEVICENUM	VARCHAR(21)	采集设备编码
SERVICECODE	VARCHAR(14)	场所编码
PROTOCOL_TYPE	VARCHAR(10)	协议类型，参见附录D
ACCOUNT	VARCHAR(64)	帐号
URL	VARCHAR(1024)	URL
COMPANY_ID	VARCHAR(32)	厂商组织机构代码，参见附录B
AP_CHANNEL	VARCHAR(2)	接入热点频道
AP_ENCRYTYPE	VARCHAR(2)	接入热点加密类型，详见附录K
CONSULT_XPOINT	VARCHAR(8)	WIFI终端相对采集设备的X坐标(正东方向)，单位：米（m）
CONSULT_YPOINT	VARCHAR(8)	WIFI终端相对采集设备的Y坐标(正北方向)，单位：米（m）

***/

object RZXFeatureBase extends Core {

  /**
    任子行在进出站安装，所以理论持续时间为扫描时间+-LAST_TIME_IN_METRO_STATION
  **/
  case class TT(mac:String,devicenum:String,macstarttime:String,macendtime:String)
  // def formatTime(data:DataFrame) = {
  //   val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  //   val addtime = TimeDistance.addTime(format)_
  //   data.rdd.map(arr =>
  //     TT(arr(14).toString,addtime(_-_,arr(21).toString),
  //                         addtime(_+_,arr(21).toString),
  //                         arr(7).toString))
  // }

  def formatTime(data:DataFrame) = {
    data.rdd.map(arr => (arr(14).toString, List(arr(21),arr(7),arr(14)).mkString(",")))
        .groupByKey().flatMap(makeLastOneStation)
        .map(_.split(",")).map(arr =>
                          TT(arr(0),arr(1),arr(2),arr(3))
                        )
  }

  def satifyLast(s1:String,s2:String) = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val L1 = s1.split(",")
    val L2 = s2.split(",")
    if(! L1(1).equals(L2(1))) true
    else if
    (scala.math.abs(format.parse(L1(0)).getTime - format.parse(L2(0)).getTime)
                    > 10*60*1000)
                    true
    else false
  }

  def makeLastOneStation(x:(String,Iterable[String])) = {
    val arr = x._2.toArray.sortWith((x,y) => x < y)
    val res = scala.collection.mutable.ArrayBuffer[String]()
    var last:String = arr(0).split(",")(1)
    var lasttime:String = arr(0).split(",")(0)
    for(i <- 0 until arr.size - 1 ){
      if (satifyLast(arr(i),arr(i+1))){
        res += List(x._1,last,lasttime,
                    arr(i+1).split(",")(0))
                    .mkString(",")
        last = arr(i+1).split(",")(1)
        lasttime = arr(i+1).split(",")(0)
      }
    }
    if(last.equals(arr(arr.size - 1).split(",")(1))
        && lasttime.equals(arr(arr.size - 1).split(",")(0))){
          //add time
          res += List(x._1,last,TimeDistance.addTime(_-_,lasttime,1*60),
                    TimeDistance.addTime(_+_,lasttime,1*60))
                    .mkString(",")
        }
    else {
        res += List(x._1,last,lasttime,
                    arr(arr.size - 1).split(",")(0))
                    .mkString(",")
    }
    res
  }
  // case class Feature(account:java.lang.String,apchannel:java.lang.String,apencartype:java.lang.String,bssid:java.lang.String,companyid:java.lang.String
  //                   ,consultxpoint:java.lang.String,consultypoint:java.lang.String,
  //                     devicenum:java.lang.String,devmac:java.lang.String,endtime:java.lang.String,essid:java.lang.String,historyessid:java.lang.String
  //                     ,imei:java.lang.String,imsi:java.lang.String,mac:java.lang.String,model:java.lang.String,
  //                     osversion:java.lang.String,phone:java.lang.String,power:java.lang.String,protocoltype:java.lang.String,servicecode:java.lang.String
  //                     ,starttime:java.lang.String,station:java.lang.String,stype:java.lang.String,
  //                     url:java.lang.String,xpoint:java.lang.String,ypoint:java.lang.String,recieveTime:java.lang.String
  //                   )
  // def parseClass(arr : List[String]):Option[Any] = {
  //   try{
  //       Some(Feature(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8),arr(9),arr(10),arr(11),arr(12),arr(13),arr(14),arr(15),arr(16),arr(17),
  //           arr(18),arr(19),arr(20),arr(21),arr(22),arr(23),arr(24),arr(25),arr(26),arr(27)))
  //       }
  //   catch{
  //           case ex:Throwable => None
  //       }
  // }

  // def parseCoreClass(data : DataFrame) = data.rdd.map(arr => CoreClass(arr.getString(14),arr.getString(27)))

  // def parseCore2Class(data : DataFrame) = data.rdd.map(arr => CoreClass2(arr.getString(14),arr.getString(27)))

  /****
    return List
    esindex,estype,escol,esid,
  ****/
  // def Alarm(p:String):List[String] = {
  //   //加上end，scala的split不会分割出最后一直为空的逗号
  //   val feature = (p+",end").split(",") match {
  //     case arr:Array[String] if( arr.size == 29 ) => parseClass(arr.toList) match {
  //                                                                                   case Some(feature) => feature.asInstanceOf[Feature]
  //                                                                                   case _ => return List(PARSEERROR)
  //                                                                                 }
  //     case _ => return List(PARSEERROR)
  //   }
  //
  //   // make sure hit the id
  //   val warning = scala.collection.mutable.ListBuffer[String]()
  //   for ((target,record) <- Map("mac" -> feature.mac,"imei" -> feature.imei,"imsi" -> feature.imsi,"phone" -> feature.phone,"account" -> feature.account)){
  //     queryFound(PERSONALINDEX,ESTYPE,target,record) match {
  //       case Some(id) => warning += List("rzx",target,record,id._1,feature.starttime,feature.devicenum,id._2).mkString(",")
  //       case None =>
  //     }
  //   }
    // if (warning.size != 0) {
    //   return warning.toList
    // }
    //
    // queryFound(MACINDEX,ESTYPE,"mac",feature.mac) match {
    //   case Some(id) => warning += (List(MACINDEX,ESTYPE,"mac",id).mkString(",")+","+feature.toString)
    //   case None =>
    // }
    // queryFound(IMSIINDEX,ESTYPE,"imsi",feature.imsi) match {
    //   case Some(id) => warning += (List(IMSIINDEX,ESTYPE,"imsi",id).mkString(",")+","+feature.toString)
    //   case None =>
    // }
    // queryFound(IMEIINDEX,ESTYPE,"imei",feature.imei) match {
    //   case Some(id) => warning += (List(IMEIINDEX,ESTYPE,"imei",id).mkString(",")+","+feature.toString)
    //   case None =>
    // }
    // queryFound(PHONEINDEX,ESTYPE,"phone",feature.phone) match {
    //   case Some(id) => warning += (List(PHONEINDEX,ESTYPE,"phone",id).mkString(",")+","+feature.toString)
    //   case None =>
    // }
    // queryFound(ACCOUNTINDEX,ESTYPE,"account",feature.phone) match {
    //   case Some(id) => warning += (List(ACCOUNTINDEX,ESTYPE,"account",id).mkString(",")+","+feature.toString)
    //   case None =>
    // }
    // warning.toList
  // }

}
