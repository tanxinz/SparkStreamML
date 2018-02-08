package com.sibat.gongan.base

import org.apache.spark.sql.DataFrame
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast

import com.sibat.gongan.imp.Core

object TYMACBase extends Core{

  case class MAC(deviceId:java.lang.String,time:java.lang.String,status:java.lang.String,mac:java.lang.String,recieveTime:java.lang.String)

  // def trail(df:DataFrame,start:String,end:String,date:String) = {
  //   df.where("time > '"+start+"' and time <= '"+end+"'").rdd
  //           .map(arr => MAC(arr.getString(0),arr.getString(1),arr.getInt(2).toString,arr.getString(3),arr.getString(4)))
  //           .groupBy(mac => mac.mac).map{
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
              { val s = MAC(arr.getString(0),arr.getString(1),arr.getInt(2).toString,arr.getString(3),arr.getString(4))
                val put = new Put(Bytes.toBytes(s.mac+"#"+s.time))
                put.add(Bytes.toBytes("trail"),Bytes.toBytes("trail"),
                        Bytes.toBytes(s.deviceId+","+s.deviceId))
    		        (new ImmutableBytesWritable, put)
              })
  }
}
