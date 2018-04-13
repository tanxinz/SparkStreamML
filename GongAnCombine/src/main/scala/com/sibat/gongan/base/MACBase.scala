package com.sibat.gongan.base

import org.apache.spark.sql.SQLContext
import scala.collection.Seq

object MACBase {

  object V1 {
    def getDF(sqlContext:SQLContext,date:String,datapath:String) = {
      val rzx = subDF(sqlContext,datapath+"/rzx_feature/"+date,"DeviceStation/rzx","devicenum")
                .withColumnRenamed("starttime","mactime").withColumnRenamed("station","macstation")
      val ap = sqlContext.read.parquet(datapath+"/ap_point/"+date).withColumnRenamed("bid","macstation")
                .withColumnRenamed("stime","mactime")
      rzx.select("mac","macstation","mactime").union(ap.select("mac","macstation","mactime")).filter("mac != 'null'")
    }

    def subDF(sqlContext:SQLContext,datapath:String,locationpath:String,locationcol:String) = {
      val location = sqlContext.read.parquet(locationpath)
      val df = sqlContext.read.parquet(datapath).drop("station")
      df.join(location,Seq(locationcol),"inner")
    }
  }

  object V2 {
    def getDF(sqlContext:SQLContext,date:String,datapath:String) = {
      import sqlContext.implicits._
      val rzx = subDF(sqlContext,datapath+"/rzx_feature/"+date,"DeviceStation/rzx","devicenum")
      val ap = APPointBase.formatTime(sqlContext.read.parquet(datapath+"/ap_point/"+date)).toDF
      rzx.select("mac","macstation","macstarttime","macendtime").union(ap.select("mac","macstation","macstarttime","macendtime"))
         .filter("mac != 'null'")
    }

    def subDF(sqlContext:SQLContext,datapath:String,locationpath:String,locationcol:String) = {
      import sqlContext.implicits._
      val location = sqlContext.read.parquet(locationpath)
      val df = sqlContext.read.parquet(datapath).drop("station")
      RZXFeatureBase.formatTime(df).toDF.join(location,Seq(locationcol),"inner")
                    .withColumnRenamed("station","macstation")
    }
  }

}
