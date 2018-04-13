package com.sibat.gongan.base

// import com.sibat.gongan.util.EZJSONParser
// import com.sibat.gongan.imp.{Core,ESQueryTrait,CommonCoreTrait,IPropertiesTrait,StatusTrait}
//
//
object APPointBase extends Core {

  /**
    ap在室内，所以理论持续时间为该车站第一次出现的
  **/
  case class TT(mac:String,macstarttime:String,macendtime:String,macstation:String)
  def formatTime(data:DataFrame) = {
    data.rdd.map(arr => (arr(0).toString, List(arr(5),arr(1),arr(0)).mkString(","))
        .groupByKey().flatMap(makeLastOneStation)
        .map(_.split(",")).map(arr =>
                          TT(arr(0),arr(1),arr(2),arr(3))
                        )
  }

  def satifyLast(s1:String,s2:String) = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val L1 = s1.split(",")
    val L2 = s2.split(",")
    if(! L1(1).equals(L2(1))) true
    else if
    (scala.math.abs(format.format(L1(0)) - format.format(L2(0)))
                    > 10*60*1000)
                    true
    else false
  }

  def makeLastOneStation(x:(String,Iterable[String])) = {
    val arr = x._2.toArray.sortWith((x,y) => x < y)
    val res = scala.collection.mutable.ArrayBuffer[String]()
    var last:String = arr(0).split(",")(1)
    var lasttime:String = arr(0).split(",")(0)
    for(i <- 0 until arr.size - 1 ){
      if satifyLast(arr(i),arr(i+1)){
        res += List(x._1,last,lasttime,
                    arr(i+1).split(",")(0))
                    .mkString(",")
        last = arr(i+1).split(",")(1)
        lasttime = arr(i+1).split(",")(0)
    }
    if(last.equals(arr(i+1).split(",")(1))
        && lasttime.equals(arr(i+1).split(",")(0))){
          //add time
          res += List(x._1,last,TimeDistance.addTime(_-_,lasttime,2*60),
                    TimeDistance.addTime(_+_,lasttime,2*60))
                    .mkString(",")
        }
    else {
        res += List(x._1,last,lasttime,
                    arr(i+1).split(",")(1),arr(i+1).split(",")(0))
                    .mkString(",")
    }
    res
  }


//
//   case class Point(mac:java.lang.String,bid:java.lang.String,fid:java.lang.String,aid:java.lang.String,apid:java.lang.String,
//     stime:java.lang.String,longtitude:java.lang.String,latitude:java.lang.String,recieveTime:java.lang.String)
//
//   def parseClass(arr : List[String]):Option[Any] = {
//     try{
//         Some(Point(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8)))
//         }
//     catch{
//             case ex:Throwable => None
//         }
//   }
//
//   /****
//     return List
//     esindex,estype,escol,esid,
//   ****/
//   def Alarm(p:String):List[String] = {
//     //加上end，scala的split不会分割出最后一直为空的逗号
//     val ap = (p+",end").split(",") match {
//       case arr:Array[String] if( arr.size == 10 ) => parseClass(arr.toList) match {
//                                                                                     case Some(ap) => ap.asInstanceOf[Point]
//                                                                                     case _ => return List(PARSEERROR)
//                                                                                   }
//       case _ => return List(PARSEERROR)
//     }
//
//     // make sure hit the id
//     val warning = scala.collection.mutable.ListBuffer[String]()
//     queryFound(PERSONALINDEX,ESTYPE,"mac",ap.mac) match {
//       case Some(id) => warning += List("ap","mac",ap.mac,id._1,ap.stime,ap.apid,ap.bid,id._2).mkString(",")
//       case None =>
//     }
//     // if (warning.size != 0) {
//     //   return warning.toList
//     // }
//     //
//     // queryFound(MACINDEX,ESTYPE,"mac",ap.mac) match {
//     //   case Some(id) => warning += (List(MACINDEX,ESTYPE,"mac",id).mkString(",")+","+ap.toString)
//     //   case None =>
//     // }
//     warning.toList
//   }
//
}
