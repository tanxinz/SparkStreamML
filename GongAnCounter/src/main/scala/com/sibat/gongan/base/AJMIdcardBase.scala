package com.sibat.gongan.base

import org.apache.spark.sql.DataFrame
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes

import com.sibat.gongan.imp.Core


// object AJMIdcardBase extends Core {
//
//   case class Idcard(name:java.lang.String,sex:java.lang.String,nation:java.lang.String,birth:java.lang.String,
//                   addr:java.lang.String,idno:java.lang.String,authority:java.lang.String,validBegin:java.lang.String,validEnd:java.lang.String,
//                   face:java.lang.String,recieveTime:java.lang.String)
//
//   def trail(df:DataFrame,start:String,end:String,date:String) = {
//     df.where("recieveTime > '"+start+"' and recieveTime <= '"+end+"'").rdd
//             .map(arr =>{
//               val s = Idcard(arr.getString(0),arr.getString(1),arr.getString(2),arr.getString(3),
//                             arr.getString(4),arr.getString(5),arr.getString(6),arr.getString(7),
//                             arr.getString(8),arr.getString(9),arr.getString(10))
//               val put = new Put(Bytes.toBytes(s.idno+"-"+s.recieveTime))
//               put.add(Bytes.toBytes("trail"),Bytes.toBytes("trail"),
//                         Bytes.toBytes(s.+","+s.lte_dev_code))
//               (new ImmutableBytesWritable, put)
//             })
//   }
//
// }
