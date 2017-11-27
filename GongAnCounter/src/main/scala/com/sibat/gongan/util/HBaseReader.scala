package com.sibat.gongan.util

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.client.{HBaseAdmin,HTable,Put,Result,Scan,Get}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat,TableOutputFormat}
import org.apache.hadoop.hbase.util.{Bytes,Base64}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark._
import scala.collection.JavaConverters._

import com.sibat.gongan.imp.IPropertiesTrait

object HBaseReader extends IPropertiesTrait {

	def hbaseConfInit(quorum:String)(port:String) = {
		val conf = HBaseConfiguration.create()
		//设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum",quorum)
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", port)
		conf.set("hbase.master", "node1:600000")
		//返回conf
		conf
	}

	def convertScanToString(scan:Scan) = {
		val proto = ProtobufUtil.toScan(scan)
		Base64.encodeBytes(proto.toByteArray)
	}

	def getToday() = {
			val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
			val now = format.format(new java.util.Date())
			(now+" 00:00:00",now+" 23:59:59")
	}

	def read(sc:SparkContext,table:String) = {

		val conf = hbaseConfInit(ZOOKEEPEQUORUM)(ZOOKEEPERPORT)

		conf.set(TableInputFormat.INPUT_TABLE, table)

		val scan = new Scan()
		val today = getToday()
		scan.setStartRow(today._1.getBytes)
		scan.setStopRow(today._2.getBytes)
		conf.set(TableInputFormat.SCAN,convertScanToString(scan))
		val rdd = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
		  classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
		  classOf[org.apache.hadoop.hbase.client.Result])

		rdd.map(tuple => tuple._2)
	}


}
