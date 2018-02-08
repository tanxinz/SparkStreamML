package com.sibat.gongan.base

import org.apache.spark.sql.DataFrame
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast

import com.sibat.gongan.imp.Core

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

  case class Feature(account:java.lang.String,apchannel:java.lang.String,apencartype:java.lang.String,bssid:java.lang.String,companyid:java.lang.String
                    ,consultxpoint:java.lang.String,consultypoint:java.lang.String,
                      devicenum:java.lang.String,devmac:java.lang.String,endtime:java.lang.String,essid:java.lang.String,historyessid:java.lang.String
                      ,imei:java.lang.String,imsi:java.lang.String,mac:java.lang.String,model:java.lang.String,
                      osversion:java.lang.String,phone:java.lang.String,power:java.lang.String,protocoltype:java.lang.String,servicecode:java.lang.String
                      ,starttime:java.lang.String,station:java.lang.String,stype:java.lang.String,
                      url:java.lang.String,xpoint:java.lang.String,ypoint:java.lang.String,recieveTime:java.lang.String
                    )


                    def trail(df:DataFrame,start:String,end:String,date:String,devicestation:Broadcast[Map[String,Map[String,String]]]) = {
                      df.where("recieveTime > '"+start+"' and recieveTime <= '"+end+"'").rdd
                              .map(arr =>{
                                val s = Feature(arr.getString(0),arr.getString(1),arr.getString(2),arr.getString(3),
                                              arr.getString(4),arr.getString(5),arr.getString(6),arr.getString(7),
                                              arr.getString(8),arr.getString(9),arr.getString(10),arr.getString(11),
                                              arr.getString(12),arr.getString(13),arr.getString(14),arr.getString(15),
                                              arr.getString(16),arr.getString(17),arr.getString(18),arr.getString(19),
                                              arr.getString(20),arr.getString(21),arr.getString(22),arr.getString(23),
                                              arr.getString(24),arr.getString(25),arr.getString(26),arr.getString(27))
                                val put = new Put(Bytes.toBytes(s.mac+"#"+s.starttime+"#rzx"))
                                put.add(Bytes.toBytes("trail"),Bytes.toBytes("trail"),
                                        Bytes.toBytes(devicestation.value("rzx")(s.devicenum)+","+s.devicenum))
                                (new ImmutableBytesWritable, put)
                              })
                    }

  // def stamp2Time(timeStamp:String) = {
  //   try{
  //     val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
	// 	  format.format(new java.util.Date(timeStamp.toLong*1000))
  //   }catch {
  //     case e:Exception => timeStamp
  //   }
  // }

}
