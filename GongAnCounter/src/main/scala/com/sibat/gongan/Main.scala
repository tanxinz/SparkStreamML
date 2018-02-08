package com.sibat.gongan

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.hbase.client.{Put,Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.broadcast.Broadcast
import java.util.Properties
import org.apache.spark.sql.SaveMode

import scala.util.Try

import com.sibat.gongan.util._
import com.sibat.gongan.imp._
import com.sibat.gongan.base._

/******************************
	统计指标
******************************/

object Main extends IPropertiesTrait {

	def sparkInit(appName:String):SparkContext = new SparkContext(new SparkConf().setAppName(appName).set("spark.cores.max","16"))

	def hbaseConfInit(quorum:String)(port:String)(master:String)(sc:SparkContext) = {
		//设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum",quorum)
    //设置zookeeper连接端口，默认2181
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", port)
		sc.hadoopConfiguration.set("hbase.master", master)
	}

	def jobInit(sc:SparkContext) = {
		val job = new Job(sc.hadoopConfiguration)
		job.setOutputKeyClass(classOf[ImmutableBytesWritable])
		job.setOutputValueClass(classOf[Result])
		job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
		job
	}

	def main(args: Array[String]): Unit = {

		val sc = sparkInit(APPNAME)

		//加载配置文件到worker上
		sc.addFile("/home/hadoop/Gongan/Counter/common.properties")

		hbaseConfInit(ZOOKEEPEQUORUM)(ZOOKEEPERPORT)(HBASEMASTER)(sc)

		//sql 生成
		val sqlContext:SQLContext = new SQLContext(sc)

		//设备站点信息
		val devicestation = sc.broadcast(initDeviceStation(sqlContext))

		// val szt = sqlContext.read.parquet("szt/"+getFloderAndFile+"/*")
		// val rdd = SZTBase.SZT(szt)
		val times = getTimes()

		val save = save2Hbase(sc,sqlContext,times,devicestation)_
		// save(TYMACBase,TYMACTABLENAME)
		// save(TYIMSIBase,MACTABLE)
		save(SZTBase,SZTTABLENAME)
		// save(AJM4GBase,AJM4GTABLE)
		// save(AJMWIFIBase,AJMWIFITABLE)
		// save(AJMAccountBase,AJMACCOUNTTABLE)
		save(APPointBase,MACTABLE)
		save(RZXFeatureBase,MACTABLE)
		save(SensorIdcardBase,IDNOTABLE)

		Try{
			val szt = sqlContext.read.parquet("/user/hadoop/GongAn/szt/"+times._1)
			// szt.groupBy("tradeAddress").count().rdd.saveAsTextFile("GongAn/sztStationCounter/"+times._1+"/"+times._2.replace(" ","-").replace(":","-"))
			import sqlContext.implicits._
			save2Hbase(sc,SZTBase.CountAll(szt,times._2.replace(" ","-").replace(":","-"),times._2),SZTCOUNTALLTABLE)
			//第一次没有sztods，文件太多会报错
			var ods:DataFrame = null
			// println("----------------------------what???????????????---------------------")
			val istt = Try{
				ods = sqlContext.read.parquet("/user/hadoop/GongAn/sztods/"+times._1)
			}
			val odf = SZTBase.ODDF(sqlContext,szt,times._3,times._2,times._1).toDF
			val nods = if(istt.isSuccess){
				odf.union(ods).groupBy("tradeAddress","tradeAddress2").agg(sum("count").alias("count"))
			}else{
				odf
			}
			// println("----------------------------fuck???????????????---------------------")
			//hbase 中的od统计表
			save2Hbase(sc,SZTBase.ODPut(nods,times._2.replace(" ","-")),ODCOUNTTABLE)
			odf.coalesce(10).write.mode(SaveMode.Append).parquet("GongAn/sztods/"+times._1)
		}

		WarnningBase.topStation5(sc,sqlContext)

		// val tymac = sqlContext.read.parquet("GongAn/ty_mac/"+times._1+"/*")
		// val rdd = TYMACBase.TYMACTrail(tymac,times._3,times._2)
		// sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, TYMACTABLENAME)
		// rdd.saveAsNewAPIHadoopDataset(jobInit(sc).getConfiguration())

		// val tymac = sqlContext.read.parquet("GongAn/ty_imsi/"+times._1+"/*")
		// val rdd = TYMACBase.TYMACTrail(tymac,times._3,times._2)
		// sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, TYIMSITABLENAME)
		// rdd.saveAsNewAPIHadoopDataset(jobInit(sc).getConfiguration())

	}

	def save2Hbase(sc:SparkContext,data:Put,tablename:String) = {
		HBaseConnectionPool.Connection(tablename).put(data)
	}

	def save2Hbase(sc:SparkContext,rdd:RDD[Tuple2[ImmutableBytesWritable,Put]],tablename:String) = {
		Try{
			sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)
			rdd.saveAsNewAPIHadoopDataset(jobInit(sc).getConfiguration())
		}
	}

	/**
	 * 保存轨迹
	**/
	def save2Hbase(sc:SparkContext,sqlContext:SQLContext,times:Tuple3[String,String,String],devicestation:Broadcast[Map[String,Map[String,String]]])
								(base:Core,tablename:String) = {
		Try{
			val data = sqlContext.read.parquet("/user/hadoop/GongAn/"+tablename+"/"+times._1)
			val rdd = base.trail(data,times._3,times._2,times._1,devicestation)
			sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)
			rdd.saveAsNewAPIHadoopDataset(jobInit(sc).getConfiguration())
		}
	}


	def getTimes() = {
		val format = new java.text.SimpleDateFormat("yyyyMMdd")
		val timeformat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
		val cal = java.util.Calendar.getInstance()
		lazy val now = new java.util.Date()
		cal.setTime(now)
		cal.add(java.util.Calendar.MINUTE,COUNTERTIMESPAN.toInt)
		(format.format(now),timeformat.format(now),timeformat.format(cal.getTime))
	}

	def initDeviceStation(sqlContext:SQLContext) = {
		val map = scala.collection.mutable.Map[String,Map[String,String]]()
		for(i <- List("rzx","ty","sensordoor","ajm")) {
			val smap = scala.collection.mutable.Map[String,String]()
			val temp = sqlContext.read.parquet("DeviceStation/"+i).rdd.map(_.mkString(",")).collect
			for (ss <- temp){
				val arr = ss.split(",")
				smap += (arr(0) -> arr(1))
			}
			map += (i -> smap.toMap)
		}
		map.toMap
	}
}
