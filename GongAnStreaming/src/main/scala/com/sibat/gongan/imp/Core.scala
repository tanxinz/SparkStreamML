package com.sibat.gongan.imp

import org.apache.hadoop.hbase.client.Put
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import scala.collection.Seq

//execute Core
abstract class Core extends Serializable{

  /*********
    query from ES to make sure it's a safe recoder
    define a alarm info and send it to kafka( or a cache
    cause that a nearly alarm happended in the short time
    and will send lots of same info to web)
  *********/
  // def Alarm(recorder:String):Map[String,String]

  def parseClass(arr : List[String]):Option[Any]


  def InnerJoin(df1:DataFrame,df2:Broadcast[DataFrame],col1:String,col2:String) = {
    val df2w = df2.value.where(col2+" != ''")
    df1.join(df2w,(df1(col1) === df2w(col2)),"inner")
  }
  /**********
    make the recorder to PUT to save it to hbase soon
  **********/
  // def FormatPut(alarm:Map[String,String]):Put


  // def ESDecoder(context:String):Map[String,Any]

}
