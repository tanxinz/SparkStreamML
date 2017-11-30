package com.sibat.gongan.base
/****
***/
import com.sibat.gongan.imp.{Core,ESQueryTrait,CommonCoreTrait,IPropertiesTrait,StatusTrait}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame

class WarnningBase(devicestation:Broadcast[Map[String,Map[String,String]]]) extends ESQueryTrait with IPropertiesTrait with CommonCoreTrait{

  // 厂商名，数据名（szt/mac/idno），数据id，id唯一标识（指的是es的id），设备编号，预警级别，预留字段
  case class Warnning(stype:String,datatype:String,dataid:String,pid:String,time:String,deviceid:String,important:String,
    reserve:String="",station:String="")

  def parse(data:String) = {
    val s = parseToClass(data.split(","))
    val put = new Put(Bytes.toBytes(s.time+"#"+s.datatype+"#"+s.dataid+"#"+s.station+"#"+s.pid))
    put.add(Bytes.toBytes("warnning"),Bytes.toBytes("warnning"),
              Bytes.toBytes(s.stype+","+s.important+","+s.deviceid+","+s.reserve))
    put
  }

  def parseToClass(data:Array[String]):Warnning = {
    //最后为，时分割不出来
    data(1) match {
      case "rzx" => Warnning(data(1),data(2),data(3),data(4),data(5),data(6),data(7),data(8)+"#"+data(9),queryStation(data(1),data(6)))
      case "ap" => Warnning(data(1),data(2),data(3),data(4),data(5),data(6),data(7),"",data(8))
      case "sensordoor" => Warnning(data(1),data(2),data(3),data(4),data(5),data(6),data(7),data(8),queryStation(data(1),data(6)))
      case "szt" => Warnning(data(1),data(2),data(3),data(4),data(5),data(6),data(7),"",data(8))
      case "ty" => Warnning(data(1),data(2),data(3),data(4),data(5),data(6),data(7),"",queryStation(data(1),data(6)))
      case _ => Warnning("","","","","","","","")
    }
  }

  def queryStation(stype:String,deviceid:String) = {
    devicestation.value(stype)(deviceid)
  }

  def toDF(sqlContext:SQLContext,data:RDD[(String,String)]) = {
    import sqlContext.implicits._
    data.map(_._2.split(",")).map(arr => parseToClass(arr)).toDF()
        .select("stype","datatype","dataid","pid","station","important","reserve")
  }

}
