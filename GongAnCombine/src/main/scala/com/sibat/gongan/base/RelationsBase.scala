package com.sibat.gongan.base

import com.sibat.gongan.util.EZJSONParser
import com.sibat.gongan.imp.{Core,ESQueryTrait,IPropertiesTrait}
import java.util.Properties
import scala.util.{Try,Failure,Success}
import org.apache.spark.sql.{SQLContext,DataFrame}
import org.apache.spark.sql.functions.{col,sum,count,abs}

import com.sibat.gongan.util.TimeDistance
import com.sibat.gongan.imp.IPropertiesTrait

object RelationBase extends IPropertiesTrait{

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
    (all.filter(arr => arr.getString(2) == arr.getString(4)).groupBy(col1,col2).count.withColumn("count",col("count") * 1.0),
     all.filter(arr => arr.getString(2) != arr.getString(4)).groupBy(col1,col2).count.withColumn("count",col("count") * 1.0))
  }


  /***
  stype:融合的数据类型
  dtype:同时同站还是同时不同站
  ***/
  def MergeHistory(sqlContext:SQLContext)(df:DataFrame,stype:String,dtype:String,combineid1:String,combineid2:String):DataFrame = {
    ReadHistory(sqlContext,stype,dtype) match {
      case x:DataFrame => x.withColumn("count",col("count") * HISTORYATTENUATION.toDouble).union(df).
                            groupBy(combineid1,combineid2).agg(sum("count").alias("count"))
      case None => df
    }
  }

  private def ReadHistory(sqlContext:SQLContext,stype:String,dtype:String) = {
    Try {
      sqlContext.read.parquet("Combine/"+dtype+"/"+ stype)
    } match {
      case Failure(_) => None
      case Success(x) => x.asInstanceOf[DataFrame]
    }

  }

  def CalculateBsRate(sqlContext:SQLContext)(same:DataFrame,diff:DataFrame,df1:DataFrame,df2:DataFrame,df1col:String,df2col:String):DataFrame = {
    val diffs = diff.withColumn("count",-col("count"))
    val merges = same.union(diffs).groupBy(df1col,df2col).agg(sum("count").alias("count"))
    val df1g = df1.groupBy(df1col).agg(count(df1col).alias(df1col+"count"))
    val df2g = df2.groupBy(df2col).agg(count(df2col).alias(df2col+"count"))
    merges.join(df1g,Seq(df1col)).join(df2g,Seq(df2col))
          .withColumn("rate",(col("count")*abs(col("count")))/(col(df1col+"count")*col(df2col+"count")))
  }

}
