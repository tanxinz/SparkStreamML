package com.sibat.gongan.base

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.{SQLContext,DataFrame}
import org.apache.spark.sql.functions.desc
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes

import com.sibat.gongan.util._

object WarnningBase {

  case class Warnning(stype:String,datatype:String,dataid:String,pid:String,time:String,deviceid:String,important:String,station:String)

  def topStation5(sc:SparkContext,sqlContext:SQLContext) = {
    import sqlContext.implicits._
    val value = HBaseReader.read(sc,"warnning").map(
          res => (res.getRow.map(_.toChar).mkString.split("#"),
                  res.getValue("warnning".getBytes(),"warnning".getBytes).map(_.toChar).mkString.split(",")))
          .map(arr => Warnning(arr._2(0),arr._1(1),arr._1(2),arr._1(4),arr._1(0),arr._2(2),arr._2(1),arr._1(3)))
          .toDF().groupBy("station").count().orderBy(desc("count"))
          .take(5).map(_.mkString(":")).mkString(",")

    val table = HBaseConnectionPool.Connection("warnning_stations")
    val put = new Put(Bytes.toBytes(getNow()))
    put.add(Bytes.toBytes("stations"),Bytes.toBytes("stations"),
              Bytes.toBytes(value))
    table.put(put)
    table.flushCommits()
    table.close()
  }

  def getNow() = {
      (Long.MaxValue - (new java.util.Date()).getTime).toString
  }

}
