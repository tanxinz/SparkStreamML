package com.sibat.gongan.base

import org.apache.spark.sql.SQLContext
import scala.collection.Seq

object IDNOBase extends Any {
  def getDF(sqlContext:SQLContext,date:String,datapath:String) = {
    val sensordoor = subDF(sqlContext,datapath+"/sensordoor_idcard/"+date,"DeviceStation/sensordoor_idcard","mac")
              .withColumnRenamed("recieveTime","idnotime").withColumnRenamed("station","idnostation")
    sensordoor.select("idno","idnostation","idnotime")
  }

  def subDF(sqlContext:SQLContext,datapath:String,locationpath:String,locationcol:String) = {
    val location = sqlContext.read.parquet(locationpath)
    val df = sqlContext.read.parquet(datapath).drop("station")
    df.join(location,Seq(locationcol),"inner")
  }

}
