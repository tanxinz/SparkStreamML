package com.sibat.gongan.util

// import org.apache.hadoop.hbase.HBaseConfiguration
// import org.apache.hadoop.hbase.client.HConnectionManager
// import org.apache.hadoop.hbase.client.HConnection

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{HBaseAdmin, HConnection, HConnectionManager, Get, Scan}
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.util.Bytes


import scala.util.Try

import com.sibat.gongan.imp.IPropertiesTrait

object  HBaseConnectionPool extends IPropertiesTrait{

  private var connection:HConnection = null

  private def InitConnection():HConnection = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", ZOOKEEPEQUORUM)
    hbaseConf.set("hbase.zookeeper.property.clientPort", ZOOKEEPERPORT)
    hbaseConf.set("hbase.defaults.for.version.skip", "true")
    HConnectionManager.createConnection(hbaseConf)
  }

  def Connection(tablename:String) = {
    if(connection == null || connection.isClosed){
      connection = InitConnection()
    }
    connection.getTable(tablename)
  }

}
