package com.sibat.gongan.base

import com.sibat.gongan.util.EZJSONParser
import com.sibat.gongan.imp.{Core,ESQueryTrait,IPropertiesTrait}
import java.util.Properties
import org.apache.spark.sql.{SQLContext,DataFrame}
import org.apache.spark.sql.functions.col

import com.sibat.gongan.util.TimeDistance

object RelationBase {

  // def calculationDistance(df1:DataFrame,df2:DataFrame) = {
  //   df1.join(df2,Seq(""),"outter")
  // }
  def CalcuteSameTime(df1:DataFrame,df2:DataFrame,time1:String,time2:String,
                      col1:String,col2:String,locationcol1:String,locationcol2:String) = {
                        //为避免在join阶段shuffle倾斜，date将会带上小时第一位，因为站点也只有一个
    val df1s = df1.withColumn("date",col(time1).substr(0,13))
    val df2s = df2.withColumn("date",col(time2).substr(0,13))
    val all = df1s.join(df2s,Seq("date"),"inner")
                  .select(df1s(col1),df2s(col2),df1s(locationcol1),df1s(time1),df2s(locationcol2),df2s(time2))
                  .filter(arr => TimeDistance.filter(arr.getString(3),arr.getString(5)))
    (all.filter(arr => arr.getString(2) == arr.getString(4)).groupBy(col1,col2).count,
     all.filter(arr => arr.getString(2) != arr.getString(4)).groupBy(col1,col2).count)
  }

}
