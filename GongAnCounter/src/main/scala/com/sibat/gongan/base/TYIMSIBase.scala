package com.sibat.gongan.base

import org.apache.spark.sql.DataFrame
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast

import com.sibat.gongan.imp.Core

object TYIMSIBase extends Core{

  case class IMSI(deviceId:java.lang.String,time:java.lang.String,imsi:java.lang.String,imei:java.lang.String,
                  location:java.lang.String,recieveTime:java.lang.String)

  // def trail(df:DataFrame,start:String,end:String,date:String) = {
  //   df.where("time > '"+start+"' and time <= '"+end+"'").rdd
  //           .map(arr => IMSI(arr.getString(0),arr.getString(1),arr.getString(2).toString,arr.getString(3),arr.getString(4),arr.getString(5)))
  //           .groupBy(imsi => imsi.imsi).map{
  //             s => {
  //               val put = new Put(Bytes.toBytes(date+"-"+s._1))
  //               put.add(Bytes.toBytes("trail"),Bytes.toBytes(end),
  //                       Bytes.toBytes(s._2.map(x => x.time+","+x.deviceId).mkString(";")))
  //   		        (new ImmutableBytesWritable, put)
  //   		       }
  //     }
  // }

  def trail(df:DataFrame,start:String,end:String,date:String,devicestation:Broadcast[Map[String,Map[String,String]]]) = {
    df.where("recieveTime > '"+start+"' and recieveTime <= '"+end+"'").rdd
            .map(arr =>
              {
                val s = IMSI(arr.getString(0),arr.getString(1),arr.getString(2).toString,arr.getString(3),arr.getString(4),arr.getString(5))
                val put = new Put(Bytes.toBytes(s.imsi+"#"+stamp2Time(s.time)+"#ty"))
                put.add(Bytes.toBytes("trail"),Bytes.toBytes("trail"),
                        Bytes.toBytes(s.deviceId+","+s.deviceId))
                (new ImmutableBytesWritable, put)
              })
  }

  def stamp2Time(timeStamp:String) = {
    try{
      val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      format.format(new java.util.Date(timeStamp.toLong*1000))
    } catch {
      case e:Exception => timeStamp
    }
  }
}
