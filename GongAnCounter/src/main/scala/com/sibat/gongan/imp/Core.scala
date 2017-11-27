package com.sibat.gongan.imp

import org.apache.spark.sql.DataFrame
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD

//execute Core
abstract class Core extends Serializable{

  /*********
  *********/
  def trail(df:DataFrame,start:String,end:String,date:String):RDD[Tuple2[ImmutableBytesWritable,Put]]

  /**********
  **********/

}
