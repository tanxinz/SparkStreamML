package com.sibat.gongan.base

import org.apache.spark.sql.DataFrame
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes

import com.sibat.gongan.imp.Core

object SensorIdcardBase extends Core {

  case class Idcard(mac:java.lang.String,genId:java.lang.String,timeStamp:java.lang.String,idno:java.lang.String,name:java.lang.String
                    ,sexCode:java.lang.String,nationCode:java.lang.String,
                    birth:java.lang.String,addr:java.lang.String,authority:java.lang.String,validBegin:java.lang.String
                    ,validEnd:java.lang.String,face:java.lang.String,recieveTime:java.lang.String)


                    def trail(df:DataFrame,start:String,end:String,date:String) = {
                      df.where("recieveTime > '"+start+"' and recieveTime <= '"+end+"'").rdd
                              .map(arr =>{
                                val s = Idcard(arr.getString(0),arr.getString(1),stamp2Time(arr.getString(2)),arr.getString(3),
                                              arr.getString(4),arr.getString(5),arr.getString(6),arr.getString(7),
                                              arr.getString(8),arr.getString(9),arr.getString(10),arr.getString(11),
                                              arr.getString(12),arr.getString(13))
                                val put = new Put(Bytes.toBytes(s.idno+"#"+s.timeStamp+"#sensordoor"))
                                put.add(Bytes.toBytes("trail"),Bytes.toBytes("trail"),
                                          Bytes.toBytes(s.mac+","+s.mac+","+s.genId))
                                (new ImmutableBytesWritable, put)
                              })
                    }

              def stamp2Time(timeStamp:String) = {
                try{
                  val sformat = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
                  val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            		  format.format(sformat.parse(timeStamp))
                }catch {
                  case e:Exception => timeStamp
                }
              }


}
