package com.sibat.gongan.base

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import scala.collection.Seq
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SQLContext

import com.sibat.gongan.imp.Core

object SZTBase extends Core{

  case class SZT(id:java.lang.String,cardId:java.lang.String,tradeDate:java.lang.String,tradeAddress:java.lang.String,
                tradeType:java.lang.String,startAddress:java.lang.String,destination:java.lang.String,
                terminal:java.lang.String,recieveDate:java.lang.String)

                case class SZT2(id:java.lang.String,cardId:java.lang.String,tradeDate:java.lang.String,tradeAddress2:java.lang.String,
                              tradeType:java.lang.String,startAddress:java.lang.String,destination:java.lang.String,
                              terminal:java.lang.String,recieveDate:java.lang.String)

  // case class OD(cardId:java.lang.String,sTime:java.lang.String,sStation:java.lang.String,sTerm:java.lang.String,
                // eTime:java.lang.String,eStation:java.lang.String,eTerm:java.lang.String,difftime:java.lang.String)

                case class OD(sStation:java.lang.String,eStation:java.lang.String,num:java.lang.String)


/*****
  轨迹亿级别，所以使用宽表，在查询部分截取
******/
  // def trail(df:DataFrame,start:String,end:String,date:String) = {
  //   df.where("tradeDate >= '"+start+"' and tradeDate <= '"+end+"'").rdd
  //           .map(arr => SZT(arr.getString(0),arr.getString(1),arr.getString(2),arr.getString(3),
  //                           arr.getString(4),arr.getString(5),arr.getString(6),arr.getString(7),arr.getString(8)))
  //           .groupBy(x => x.cardId).map{
  //             s => {
  //               val put = new Put(Bytes.toBytes(date+"-"+s._1))
  //               put.add(Bytes.toBytes("trail"),Bytes.toBytes(end),
  //                       Bytes.toBytes(s._2.map(x => x.tradeDate+","+x.tradeAddress).mkString(";")))
  //   		        (new ImmutableBytesWritable, put)
  //   		       }
  //     }
  // }

  def trail(df:DataFrame,start:String,end:String,date:String) = {
    df.where("recieveDate >= '"+start+"' and recieveDate <= '"+end+"'").rdd
            .map(
              arr => {
                val s = SZT(arr.getString(0),arr.getString(1),arr.getString(2),arr.getString(3),
                            arr.getString(4),arr.getString(5),arr.getString(6),arr.getString(7),arr.getString(8))
                val put = new Put(Bytes.toBytes(s.cardId+"#"+s.tradeDate))
                put.add(Bytes.toBytes("trail"),Bytes.toBytes("trail"),
                        Bytes.toBytes(s.tradeAddress+","+s.terminal))
    		        (new ImmutableBytesWritable, put)
      })
  }


  def CountAll(df:DataFrame,time:String,end:String) = {
    val times = getTimes(15)._2
    val num = df.where("recieveDate > '" +times+"' and recieveDate <= '"+end+"'").where("tradeType == '21'").count()
    val put = new Put(Bytes.toBytes(time))
    put.add(Bytes.toBytes("count"),Bytes.toBytes("count"),
            Bytes.toBytes(num.toString))
    put
  }

  // def counter(df:DataFrame,start:String,end:String,date:String) = {
    // df.where("time >= '"+start+"' and time <= '"+end+"'").
  // }

  def ODDF(sqlContext:SQLContext,df:DataFrame,start:String,end:String,date:String) = {
    import sqlContext.implicits._
    val times = getTimes(120)
    val szt = df.where("tradeDate >= '"+start+"' and tradeDate < '"+end+"'" +"and tradeType == '22'")
    // .union(
    val in = df.where("tradeDate >= '" +times._2+"' and tradeDate < '"+times._1+"'" +"and tradeType == '21'").rdd
        .map( arr =>
            SZT2(arr.getString(0),arr.getString(1),arr.getString(2),arr.getString(3),
                      arr.getString(4),arr.getString(5),arr.getString(6),arr.getString(7),arr.getString(8))
                    )
                    .toDF()
    szt.join(in,Seq("cardId","cardId"),"inner").filter("tradeAddress2 != tradeAddress").groupBy("tradeAddress2","tradeAddress").count()
    // ODFilter.getOD(szt).map(_.split(","))
            // .map(arr => OD(arr(0).asInstanceOf[java.lang.String],arr(1).asInstanceOf[java.lang.String],arr(2).asInstanceOf[java.lang.String]
                          // ,arr(3).asInstanceOf[java.lang.String],arr(4).asInstanceOf[java.lang.String],arr(5).asInstanceOf[java.lang.String]
                          // ,arr(6).asInstanceOf[java.lang.String],arr(7).asInstanceOf[java.lang.String]))

    // ODFilter.getOD(szt).map(_.split(","))
            // .map(arr => OD(arr(0).asInstanceOf[java.lang.String],arr(1).asInstanceOf[java.lang.String],arr(2).asInstanceOf[java.lang.String]
                          // ))
  }

  // def ODDF(df:DataFrame,start:String,end:String,date:String) = {
  //   val times = getTimes(90)
  //   val szt = df.where("tradeDate >= '" +times._2+"' and tradeDate < '"+times._1+"'" ).rdd
  //   ODFilter.getOD(szt).map(_.split(","))
  //           .map(arr => OD(arr(0).asInstanceOf[java.lang.String],arr(1).asInstanceOf[java.lang.String],arr(2).asInstanceOf[java.lang.String]
  //                         ,arr(3).asInstanceOf[java.lang.String],arr(4).asInstanceOf[java.lang.String],arr(5).asInstanceOf[java.lang.String]
  //                         ,arr(6).asInstanceOf[java.lang.String],arr(7).asInstanceOf[java.lang.String]))
  // }
  //
  // def ODPut(df:DataFrame,time:String) = {
  //   df.groupBy("sStation","eStation").count().rdd.map(
  //     odc => {
  //       val put = new Put(Bytes.toBytes(time+"-"+odc(0)+"-"+odc(1)))
  //       put.add(Bytes.toBytes("counter"),Bytes.toBytes("counter"),
  //               Bytes.toBytes(odc(2).toString.asInstanceOf[String]))
  //       (new ImmutableBytesWritable, put)
  //      }
  //   )
  // }

  def ODPut(df:DataFrame,time:String) = {
    df.rdd.map(
      odc => {
        val put = new Put(Bytes.toBytes(time+"-"+odc(0)+"-"+odc(1)))
        put.add(Bytes.toBytes("counter"),Bytes.toBytes("counter"),
                Bytes.toBytes(odc(2).toString.asInstanceOf[String]))
        (new ImmutableBytesWritable, put)
       }
    )
  }

  def getTimes(span:Int) = {
		val timeformat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
		val cal = java.util.Calendar.getInstance()
		lazy val now = new java.util.Date()
		cal.setTime(now)
		cal.add(java.util.Calendar.MINUTE,0-span)
		(timeformat.format(now),timeformat.format(cal.getTime))
	}

}

object ODFilter {

  private def ssplit(x:Row) = {
    val L = x
    (L(1).asInstanceOf[String],List(L(2),L(1),L(4),L(3),L(7)).mkString(","))
  }

  // private def delTime(t1:String,t2:String) = {
    // val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    // (sdf.parse(t2).getTime - sdf.parse(t1).getTime) / 1000
  // }

  private def ODRuler(x:String,y:String) = {
    val L1 = x.split(",")
    val L2 = y.split(",")
    // val difftime = delTime(L1(0),L2(0))
    if(
      (L2(2) == "21") &&
      /*( difftime < 10800) && */ (L1(3) != L2(3))
    ) List(L2(3),L1(3),/*(difftime.toString)*/"1").mkString(",")
    else None
  }

  private def MakeOD(x:(String,Iterable[String])) = {
    val arr = x._2.toArray
    if (arr.size > 1){
       ODRuler(arr(0),arr(1))
     }
     else None
  }

  def getOD(data:RDD[Row]) = {
    data.map(ssplit)
        .groupByKey()
        .map(MakeOD)
        .filter(x => x != None)
        .map(_.asInstanceOf[String])
  }

}

// object ODFilter {
//
//   private def ssplit(x:Row) = {
//     val L = x
//     (L(1).asInstanceOf[String],List(L(2),L(1),L(4),L(3),L(7)).mkString(","))
//   }
//
//   private def delTime(t1:String,t2:String) = {
//     val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//     (sdf.parse(t2).getTime - sdf.parse(t1).getTime) / 1000
//   }
//
//   private def ODRuler(x:String,y:String) = {
//     val L1 = x.split(",")
//     val L2 = y.split(",")
//     val difftime = delTime(L1(0),L2(0))
//     if(
//       ((L1(2) == "21") && (L2(2) == "22")) &&
//       ( difftime < 10800) && (L1(3) != L2(3))
//     ) List(L1(1),L1(0),L1(3),L1(4),L2(0),L2(3),L2(4),(difftime.toString)).mkString(",")
//     else None
//   }
//
//   private def MakeOD(x:(String,Iterable[String])) = {
//     val arr = x._2.toArray.sortWith((x,y) => x < y)
//     for{
//        i <- 0 until arr.size - 1;
//        od = ODRuler(arr(i),arr(i+1))
//      } yield od
//   }
//
//   def getOD(data:RDD[Row]) = {
//     data.map(ssplit)
//         .groupByKey()
//         .flatMap(MakeOD)
//         .filter(x => x != None)
//         .map(_.asInstanceOf[String])
//   }
//
// }
