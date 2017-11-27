package com.sibat.gongan.imp

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.filter.{PrefixFilter,RegexStringComparator,RowFilter,CompareFilter,PageFilter}
import org.apache.hadoop.hbase.client.{Get, Scan}
import com.sibat.gongan.util.HBaseConnectionPool


trait HBaseQueryTrait extends StatusTrait with IPropertiesTrait{
  def HBaseQueryByKey(tableName:String,rowkey:String) = {
    val table = HBaseConnectionPool.Connection(tableName)
    val result = table.get(new Get(Bytes.toBytes(rowkey)))
    val res = scala.collection.mutable.ListBuffer[Map[String,String]]()
    import scala.collection.JavaConversions._
    for(kv <- result.list()){
      val temp = scala.collection.mutable.Map[String,String]()
      temp += ("family" -> Bytes.toString(kv.getFamily()))
      temp += ("qualifier" ->  Bytes.toString(kv.getQualifier()))
      temp += ("value" -> Bytes.toString(kv.getValue()))
      temp += ("timestamp" -> kv.getTimestamp().toString)
      res += temp.toMap
    }
    res.toList
  }

  def HBaseScanByKey(tableName:String,start:String,stop:String) = {
    val table = HBaseConnectionPool.Connection(tableName)
    // val result = table.get(new Get(Bytes.toBytes(rowkey)))
    val scan = new Scan();
    scan.setStartRow(start.getBytes);
    scan.setStopRow(stop.getBytes);
    val result = table.getScanner(scan);
    val res = scala.collection.mutable.ListBuffer[List[Map[String,String]]]()
    import scala.collection.JavaConversions._
    for ( r <- result) {
      val ress = scala.collection.mutable.ListBuffer[Map[String,String]]()
      for (kv <- r.list())
      {
         val temp = scala.collection.mutable.Map[String,String]()
         temp += ("row" -> Bytes.toString(kv.getRow()))
         temp += ("family" -> Bytes.toString(kv.getFamily()))
         temp += ("qualifier" ->  Bytes.toString(kv.getQualifier()))
         temp += ("value" -> Bytes.toString(kv.getValue()))
         temp += ("timestamp" -> kv.getTimestamp().toString)
         ress += temp.toMap
       }
       res += ress.toList
    }
    res.toList
  }

  def HBaseScanLimit(tableName:String,limit:Int) = {
    val table = HBaseConnectionPool.Connection(tableName)
    val scan = new Scan();
    scan.setMaxResultSize(limit)
    val result = table.getScanner(scan);
    val res = scala.collection.mutable.ListBuffer[List[Map[String,String]]]()
    import scala.collection.JavaConversions._
    for ( r <- result) {
      val ress = scala.collection.mutable.ListBuffer[Map[String,String]]()
      for (kv <- r.list())
      {
         val temp = scala.collection.mutable.Map[String,String]()
         temp += ("row" -> Bytes.toString(kv.getRow()))
         temp += ("family" -> Bytes.toString(kv.getFamily()))
         temp += ("qualifier" ->  Bytes.toString(kv.getQualifier()))
         temp += ("value" -> Bytes.toString(kv.getValue()))
         temp += ("timestamp" -> kv.getTimestamp().toString)
         ress += temp.toMap
       }
       res += ress.toList
    }
    res.toList
  }

  def HBaseScanByPrefixFilter(tableName:String,prefix:String) = {
    val table = HBaseConnectionPool.Connection(tableName)
    // val result = table.get(new Get(Bytes.toBytes(rowkey)))
    val scan = new Scan();
    scan.setFilter(new PrefixFilter(prefix.getBytes()))
    val result = table.getScanner(scan);
    val res = scala.collection.mutable.ListBuffer[List[Map[String,String]]]()
    import scala.collection.JavaConversions._
    for ( r <- result) {
      val ress = scala.collection.mutable.ListBuffer[Map[String,String]]()
      for (kv <- r.list())
      {
         val temp = scala.collection.mutable.Map[String,String]()
         temp += ("row" -> Bytes.toString(kv.getRow()))
         temp += ("family" -> Bytes.toString(kv.getFamily()))
         temp += ("qualifier" ->  Bytes.toString(kv.getQualifier()))
         temp += ("value" -> Bytes.toString(kv.getValue()))
         temp += ("timestamp" -> kv.getTimestamp().toString)
         ress += temp.toMap
       }
       res += ress.toList
    }
    res.toList
  }

  /***
    按前缀分页获取
  ***/
  def HBaseScanByPrefixFilter(tableName:String,prefix:String,start:String,stop:String) = {
    val table = HBaseConnectionPool.Connection(tableName)
    // val result = table.get(new Get(Bytes.toBytes(rowkey)))
    val scan = new Scan();
    scan.setFilter(new PrefixFilter(prefix.getBytes()))
    scan.setStartRow((prefix+"#"+start).getBytes);
    scan.setStopRow((prefix+"#"+stop).getBytes);
    val result = table.getScanner(scan);
    val res = scala.collection.mutable.ListBuffer[List[Map[String,String]]]()
    import scala.collection.JavaConversions._
    for ( r <- result) {
      val ress = scala.collection.mutable.ListBuffer[Map[String,String]]()
      for (kv <- r.list())
      {
         val temp = scala.collection.mutable.Map[String,String]()
         temp += ("row" -> Bytes.toString(kv.getRow()))
         temp += ("family" -> Bytes.toString(kv.getFamily()))
         temp += ("qualifier" ->  Bytes.toString(kv.getQualifier()))
         temp += ("value" -> Bytes.toString(kv.getValue()))
         temp += ("timestamp" -> kv.getTimestamp().toString)
         ress += temp.toMap
       }
       res += ress.toList
    }
    res.toList
  }

  def HBaseScanByRegexFilter(tableName:String,regex:String) = {
    val table = HBaseConnectionPool.Connection(tableName)
    // val result = table.get(new Get(Bytes.toBytes(rowkey)))
    val scan = new Scan();
    scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(regex)))
    // scan.setFilter(new PrefixFilter(prefix.getBytes()))
    val result = table.getScanner(scan);
    val res = scala.collection.mutable.ListBuffer[List[Map[String,String]]]()
    import scala.collection.JavaConversions._
    for ( r <- result) {
      val ress = scala.collection.mutable.ListBuffer[Map[String,String]]()
      for (kv <- r.list())
      {
         val temp = scala.collection.mutable.Map[String,String]()
         temp += ("row" -> Bytes.toString(kv.getRow()))
         temp += ("family" -> Bytes.toString(kv.getFamily()))
         temp += ("qualifier" ->  Bytes.toString(kv.getQualifier()))
         temp += ("value" -> Bytes.toString(kv.getValue()))
         temp += ("timestamp" -> kv.getTimestamp().toString)
         ress += temp.toMap
       }
       res += ress.toList
    }
    res.toList
  }

  def HBaseQueryNewest(tableName:String,start:String,stop:String,regex:String) = {
    val table = HBaseConnectionPool.Connection(tableName)
    // val result = table.get(new Get(Bytes.toBytes(rowkey)))
    val scan = new Scan();
    scan.setStartRow(start.getBytes);
    scan.setStopRow(stop.getBytes);
    scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(regex)))
    val result = table.getScanner(scan);
    val res = scala.collection.mutable.ListBuffer[List[Map[String,String]]]()
    import scala.collection.JavaConversions._
    for ( r <- result) {
      val ress = scala.collection.mutable.ListBuffer[Map[String,String]]()
      for (kv <- r.list())
      {
         val temp = scala.collection.mutable.Map[String,String]()
         temp += ("row" -> Bytes.toString(kv.getRow()))
         temp += ("family" -> Bytes.toString(kv.getFamily()))
         temp += ("qualifier" ->  Bytes.toString(kv.getQualifier()))
         temp += ("value" -> Bytes.toString(kv.getValue()))
         temp += ("timestamp" -> kv.getTimestamp().toString)
         ress += temp.toMap
       }
       res += ress.toList
    }
    res.toList
  }
}
