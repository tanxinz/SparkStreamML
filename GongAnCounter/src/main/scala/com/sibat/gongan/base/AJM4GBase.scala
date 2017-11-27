package com.sibat.gongan.base

import org.apache.spark.sql.DataFrame
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes

import com.sibat.gongan.imp.Core


object AJM4GBase extends Core {

  case class A4G(lte_dev_code:java.lang.String,imsi:java.lang.String,cap_time:java.lang.String,recieveTime:java.lang.String)

  def trail(df:DataFrame,start:String,end:String,date:String) = {
    df.where("recieveTime > '"+start+"' and recieveTime <= '"+end+"'").rdd
            .map(arr =>{
              val s = A4G(arr.getString(0),arr.getString(1),arr.getInt(2).toString,arr.getString(3))
              val put = new Put(Bytes.toBytes(s.imsi+"#"+s.cap_time))
              put.add(Bytes.toBytes("trail"),Bytes.toBytes("trail"),
                        Bytes.toBytes(s.lte_dev_code+","+s.lte_dev_code))
              (new ImmutableBytesWritable, put)
            })
  }

}
