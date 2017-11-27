package com.sibat.gongan.base
/****
***/
import com.sibat.gongan.imp.{Core,ESQueryTrait,CommonCoreTrait,IPropertiesTrait,StatusTrait}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

object WarnningBase extends ESQueryTrait with IPropertiesTrait with CommonCoreTrait{

  // 厂商名，数据名（szt/mac/idno），数据id，id唯一标识（指的是es的id），设备编号，预警级别，预留字段
  case class Warnning(stype:String,datatype:String,dataid:String,pid:String,time:String,deviceid:String,important:String,
    reserve:String="")

  def parse(data:String) = {
    val arr = data.split(",")
    val s = Warnning(arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8))
    val put = new Put(Bytes.toBytes(s.time+"#"+s.datatype+"#"+s.dataid+"#"+s.deviceid+"#"+s.pid))
    put.add(Bytes.toBytes("warnning"),Bytes.toBytes("warnning"),
              Bytes.toBytes(s.stype+","+s.important+","+s.reserve))
    put
  }

  def toDF(sqlContext:SQLContext,data:RDD[(String,String)]) = {
    import sqlContext.implicits._
    data.map(_._2.split(",")).map(arr => Warnning(arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8))).toDF()
        .select("stype","datatype","dataid","pid","deviceid","important","reserve")
  }

}
