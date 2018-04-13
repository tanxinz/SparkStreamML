package com.sibat.gongan.base

import org.apache.spark.sql.SQLContext
import scala.collection.Seq
import org.apache.spark.sql.DataFrame
import com.sibat.gongan.util.TimeDistance

object IDNOBase {

  object V1 {

    def getDF(sqlContext:SQLContext,date:String,datapath:String) = {
      val sensordoor = subDF(sqlContext,datapath+"/sensordoor_idcard/"+date,"DeviceStation/sensordoor","mac")
                .withColumnRenamed("recieveTime","idnotime").withColumnRenamed("station","idnostation")
      sensordoor.select("idno","idnostation","idnotime").filter("idno != 'null'")
    }

    def subDF(sqlContext:SQLContext,datapath:String,locationpath:String,locationcol:String) = {
      val location = sqlContext.read.parquet(locationpath)
      val df = sqlContext.read.parquet(datapath).drop("station")
      df.join(location,Seq(locationcol),"inner")
    }
  }

  object V2{
    def getDF(sqlContext:SQLContext,date:String,datapath:String) = {
      val sensordoor = subDF(sqlContext,datapath+"/sensordoor_idcard/"+date,"DeviceStation/sensordoor","mac")
                       .withColumnRenamed("station","idnostation")
      sensordoor.select("idno","idnostation","idnotime").filter("idno != 'null'")
    }

    def subDF(sqlContext:SQLContext,datapath:String,locationpath:String,locationcol:String) = {
      import sqlContext.implicits._
      val location = sqlContext.read.parquet(locationpath)
      val df = sqlContext.read.parquet(datapath).drop("station")
      formatTime(df).toDF.join(location,Seq(locationcol),"inner")
    }

    case class TT(idno:String,idnostarttime:String,idnoendtime:String,mac:String)
    def formatTime(data:DataFrame) = {
      val format = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
      val addtime = TimeDistance.addTime(format)_
      data.rdd.map(arr =>
        TT(arr(3).toString,addtime(_-_,arr(2).toString),
                           addtime(_+_,arr(2).toString),
                           arr(0).toString))
    }
  }
}
