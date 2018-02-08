package com.sibat.gongan.base

import org.apache.spark.sql.DataFrame
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast

import com.sibat.gongan.imp.Core


object AJMAccountBase extends Core {

  case class Account(device_mac:java.lang.String,app_code:java.lang.String,internal_id:java.lang.String,user_account:java.lang.String,
                  nick_name:java.lang.String,phone:java.lang.String,imsi:java.lang.String,imei:java.lang.String,cap_time:java.lang.String,
                  ap_mac:java.lang.String,coordinate:java.lang.String,dev_addr:java.lang.String,recieveTime:java.lang.String)


          def trail(df:DataFrame,start:String,end:String,date:String,devicestation:Broadcast[Map[String,Map[String,String]]]) = {
            df.where("recieveTime > '"+start+"' and recieveTime <= '"+end+"'").rdd
                    .map(arr =>{
                      val s = Account(arr.getString(0),arr.getString(1),arr.getString(2),arr.getString(3),
                                      arr.getString(4),arr.getString(5),arr.getString(6),arr.getString(7),
                                      arr.getString(8),arr.getString(9),arr.getString(10),arr.getString(11),
                                      arr.getString(12))
                      val put = new Put(Bytes.toBytes(s.device_mac+"#"+s.cap_time))
                      put.add(Bytes.toBytes("trail"),Bytes.toBytes("trail"),
                                Bytes.toBytes(s.ap_mac+","+s.ap_mac))
                      (new ImmutableBytesWritable, put)
                    })
          }

}
