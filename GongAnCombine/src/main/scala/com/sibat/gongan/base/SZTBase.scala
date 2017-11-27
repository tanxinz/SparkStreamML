package com.sibat.gongan.base
//
// import com.sibat.gongan.util.EZJSONParser
// import com.sibat.gongan.imp.{Core,ESQueryTrait,CommonCoreTrait,IPropertiesTrait,StatusTrait}
//
//
// object SZTBase extends Core with ESQueryTrait with IPropertiesTrait with CommonCoreTrait{
//
//   case class SZT(id:java.lang.String,cardId:java.lang.String,tradeDate:java.lang.String,tradeAddress:java.lang.String,tradeType:java.lang.String,
//                   startAddress:java.lang.String,destination:java.lang.String,terminal:java.lang.String,recieveDate:java.lang.String)
//
//
//   def parseClass(arr : List[String]):Option[Any] = {
//     try{
//         Some(SZT(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8)))
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
//     val szt = (p+",end").split(",") match {
//       case arr:Array[String] if( arr.size == 10 ) => parseClass(arr.toList) match {
//                                                                                     case Some(x) => x.asInstanceOf[SZT]
//                                                                                     case _ => return List(PARSEERROR)
//                                                                                   }
//       case _ => return List(PARSEERROR)
//     }
//
//     // make sure hit the id
//     val warning = scala.collection.mutable.ListBuffer[String]()
//     queryFound(PERSONALINDEX,ESTYPE,"szt",szt.cardId) match {
//       case Some(id) => warning += List("szt","szt",szt.cardId,id._1,szt.tradeDate,szt.terminal,id._2).mkString(",")+","+szt.toString
//       case None =>
//     }
//     // if (warning.size != 0) {
//     //   return warning.toList
//     // }
//
//     // queryFound(SZTINDEX,ESTYPE,"szt",szt.cardId) match {
//     //   case Some(id) => warning += (List(SZTINDEX,ESTYPE,"szt",id).mkString(",")+","+szt.toString)
//     //   case None =>
//     // }
//     warning.toList
//   }
//
// }
