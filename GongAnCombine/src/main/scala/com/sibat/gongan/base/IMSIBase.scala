package com.sibat.gongan.base

import org.apache.spark.sql.SQLContext
import scala.collection.Seq

object IMSIBase  {
  def getDF(sqlContext:SQLContext,date:String,datapath:String) = {
    val ty = subDF(sqlContext,datapath+"/ty_imsi/"+date,"DeviceStation/ty_imsi","deviceId")
              .withColumnRenamed("time","imsitime").withColumnRenamed("station","imsistation")
    ty.select("imsi","imsistation","imsitime").filter("imsi != 'null'")
  }

  def subDF(sqlContext:SQLContext,datapath:String,locationpath:String,locationcol:String) = {
    val location = sqlContext.read.parquet(locationpath)
    val df = sqlContext.read.parquet(datapath).drop("station")
    df.join(location,Seq(locationcol),"inner")
  }

}
