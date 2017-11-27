package com.sibat.gongan.base

import org.apache.spark.sql.DataFrame
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes

import com.sibat.gongan.imp.Core

object APPointBase extends Core{

  case class Point(mac:java.lang.String,bid:java.lang.String,fid:java.lang.String,aid:java.lang.String,apid:java.lang.String,
    stime:java.lang.String,longtitude:java.lang.String,latitude:java.lang.String,recieveTime:java.lang.String)

    def trail(df:DataFrame,start:String,end:String,date:String) = {
      df.where("recieveTime > '"+start+"' and recieveTime <= '"+end+"'").rdd
              .map(arr =>{
                val s = Point(long2Mac(arr.getString(0)),arr.getString(1),arr.getString(2),arr.getString(3),
                              arr.getString(4),arr.getString(5),arr.getString(6),arr.getString(7),
                              arr.getString(8))
                val put = new Put(Bytes.toBytes(s.mac+"#"+s.stime+"#ap"))
                put.add(Bytes.toBytes("trail"),Bytes.toBytes("trail"),
                          Bytes.toBytes(s.bid+","+s.apid))
                (new ImmutableBytesWritable, put)
              })
    }

  // def trail(df:DataFrame,start:String,end:String,date:String) = {
    // df.where("time > '"+start+"' and time <= '"+end+"'").rdd
            // .map(arr => Point(arr.getString(0),arr.getString(1),arr.getInt(2).toString,arr.getString(3),arr.getString(4),arr.getString(5)
                            // ,arr.getString(6),arr.getString(7),arr.getString(8)))
//             .groupBy(point => point.mac).map{
//               s => {
//                 val put = new Put(Bytes.toBytes(date+"-"+s._1))
//                 put.add(Bytes.toBytes("trail"),Bytes.toBytes(end),
//                         Bytes.toBytes(s._2.map(x => x.stime+","+x.apid).mkString(";")))
//     		        (new ImmutableBytesWritable, put)
//     		       }
//       }
//   }
  def long2Mac(long:String) = {
    try{
      long.toLong.toHexString.toUpperCase()
    } catch {
      case e :Exception => long
    }
  }
}
