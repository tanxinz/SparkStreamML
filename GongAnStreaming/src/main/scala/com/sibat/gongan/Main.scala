package com.sibat.gongan

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.hadoop.hbase.client.{Put,Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.{StreamingContext,Seconds,Minutes}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import scala.util.Try
import java.util.Properties

import com.sibat.gongan.util._
import com.sibat.gongan.imp._
import com.sibat.gongan.base._


object Main extends IPropertiesTrait with StatusTrait with CommonCoreTrait{

	def sparkInit(appName:String):SparkContext = new SparkContext(new SparkConf().setAppName(appName).set("spark.cores.max","16"))

	def sparkStreamingInit(sc:SparkContext)(sec:Int)():StreamingContext = {
		val ssc = new StreamingContext(sc,Seconds(sec))
		ssc.checkpoint("checkpoint")
		ssc
	}

	def kafkaProducerConfig(brokers:String) = {
    	val p = new Properties()
    	p.setProperty("bootstrap.servers", brokers)
    	p.setProperty("key.serializer", classOf[StringSerializer].getName)
    	p.setProperty("value.serializer", classOf[StringSerializer].getName)
    	p
  	}

	def main(args: Array[String]): Unit = {

		val sc = sparkInit(APPNAME)

		//加载配置文件到worker上
		sc.addFile("/home/hadoop/Gongan/Streaming/common.properties")

		//sql 生成
		val sqlContext:SQLContext = new SQLContext(sc)

		val ssc = StreamingContext.getOrCreate("checkpoint",sparkStreamingInit(sc)(APPSTREAMINGSPAN.toInt) _)

		// 广播KafkaSink
		val kafkaProducer: Broadcast[KafkaSink[String, String]] = ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig(KAFKABROKERS)))

		val connectionProperties = new Properties()
		//增加数据库的用户名(user)密码(password),指定postgresql驱动(driver)
		connectionProperties.put("user",POSTGRESUSER);
		connectionProperties.put("password",POSTGRESPASSWD);
		connectionProperties.put("driver","org.postgresql.Driver");

		// KafkaReader.Read(ssc,KAFKABROKERS,INPUTTOPICS).
		// 	foreachRDD({
		// 		rdd => {
		// 			if(!rdd.isEmpty){
		// 			rdd.foreachPartition(
		// 				partitionrdd => {
		// 					val table = HBaseConnectionPool.Connection(HBASETABLENAME)
		// 					partitionrdd.foreach(
		// 					 	p =>{
		// 								val alarm:Map[String,String] = SZTBase.Alarm(p._2.toString)
		//
		// 								kafkaProducer.value.send(OUTPUTTOPICS, alarm("status"))
		// 								// write to hbase
		// 								if(alarm("status") == "Hit")  table.put(SZTBase.FormatPut(alarm))
		// 						})
		// 					table.flushCommits()
		// 					table.close()
		// 					})
		// 				}
		// 			}
		// 		})
		// 重点人员
		val personal = sc.broadcast(sqlContext.read.parquet("Personal"))
		//设备站点信息
		val devicestation = sc.broadcast(initDeviceStation(sqlContext))

		//不能放在这里，这样只有初始化的时候会生成日期，做不到每天动态生成文件夹
		// val floderandfile = getFloderAndFile

		val todf = toDF(sqlContext)_

		// val warnningBase = new WarnningBase(devicestation)
		WarnningBase.setDeviceStationMap(devicestation)

		Try{
			for(topic <- INPUTTOPICS.split(",")){
					KafkaReader.Read(ssc,KAFKABROKERS,topic).
						foreachRDD({
							rdd => {
								if(!rdd.isEmpty){
								// rdd.foreachPartition(
								// 	partitionrdd => {
								// 		partitionrdd.foreach(
								// 		 	p =>{
								// 				val alarm = Alarm(topic,p._2.toString)
								// 				if(alarm != null){
								// 					for(i <- alarm){
								// 						i match {
								// 							case PARSEERROR =>
								// 							case info:String => kafkaProducer.value.send(WARNTEMPTOPICS, topic+","+info)
								// 							case _ =>
								// 						}
								// 					}
								// 				}
								// 			})
								// 		}
								// 	)
										println("-------------------------------------------------")
										println(rdd.count())
										//存储rdd为parquet
										val floderandfile = getFloderAndFile
										val df = todf(topic,rdd.map(_._2.toString))
										val alarm = Alarm(topic,df,personal)
										if((alarm != null )&&(alarm.size != 0 )){
											for (i <- alarm) kafkaProducer.value.send(WARNTEMPTOPICS, topic + ","+ i)
										}
										println("!!!!!"+floderandfile._1+")))"+topic)
										topic match {
											case "trail" => df.write.mode(SaveMode.Append).parquet("GongAn/rzx_"+topic+"/"+floderandfile._1)
											case _ => df.write.mode(SaveMode.Append).parquet("GongAn/"+topic+"/"+floderandfile._1)
										}
									}
								}
							})
						}
					}

			// val table = HBaseConnectionPool.Connection(WARNNINGTABLE)
			KafkaReader.Read(ssc,KAFKABROKERS,WARNTEMPTOPICS).foreachRDD({
				rdd => if(!rdd.isEmpty){
					val table = HBaseConnectionPool.Connection(WARNNINGTABLE)
					for(warn:String <- rdd.map(_._2).collect()){
							println("处理预警:"+warn);
							// table.put(warnningBase.parse(warn))
							table.put(WarnningBase.parseToPut(devicestation,warn))
						}
					table.flushCommits()
					table.close()
					// WarnningBase.toDF(sqlContext,rdd).toJSON.write.format("jdbc").mode("append")
					// 																										.option("url", "jdbc:postgresql:node2:5432/personal")
					// 																										.option("dbtable", "warnning")
					// 																										.option("user", "postgres")
					// 																										.option("password", "root")
					// 																										.save()
					// WarnningBase.toDF(sqlContext,rdd)
					// warnningBase.toDF(sqlContext,rdd).write.mode("append")
					WarnningBase.toDF(sqlContext,rdd,devicestation).write.mode("append")
        						.jdbc("jdbc:postgresql://"+POSTGRESIP+":"+POSTGRESPORT+"/"+POSTGRESDATABASE,POSTGRESTABLE,connectionProperties)
					// WarnningBase.toDF(sqlContext,ssc.sparkContext.parallelize(warnnings)).write.mode("append")
        	// 					.jdbc("jdbc:postgresql://"+POSTGRESIP+":"+POSTGRESPORT+"/"+POSTGRESDATABASE,POSTGRESTABLE,connectionProperties)
				}
			})
			// table.close()

		ssc.start()
		ssc.awaitTermination()
	}

	def Alarm(topic:String,data:DataFrame,personal:Broadcast[DataFrame]) = {
			topic match {
				case "rzx_feature" => RZXFeatureBase.Alarm(data,personal)
				case "sensordoor_idcard" => SensorIdcardBase.Alarm(data,personal)
				case "ty_imsi" => TYIMSIBase.Alarm(data,personal)
				// case "ty_mac" => TYMACBase.Alarm(data)
				// case "ajm_4g" => AJM4GBase.Alarm(data)
				// case "ajm_wifi" => AJMWIFIBase.Alarm(data)
				// case "ajm_account" => AJMAccountBase.Alarm(data)
				// case "ajm_idcard" => AJMIdcardBase.Alarm(data)
				// case "ifaas_warning" => IFAASWarningBase.Alarm(data)
				case "szt" => SZTBase.Alarm(data,personal)
				case "ap_point" => APPointBase.Alarm(data,personal)
				case _ => null
			}
	}

	def toDF(sqlContext:SQLContext)(topic:String,data:RDD[String]) = {
		import sqlContext.implicits._
		val parse = ParseClass(topic)_
		val floderandfile = getFloderAndFile
		topic match {
			case "rzx_device" => {
				data.map(_.split(",")).filter(arr => parse(arr) != null)
															.map(arr => parse(arr).asInstanceOf[RZXDeviceBase.Device]).toDF
			}
			case "rzx_feature" => {
				data.map(_.split(",")).filter(arr => parse(arr) != null)
															.map(arr => parse(arr).asInstanceOf[RZXFeatureBase.Feature]).toDF
			}
			case "rzx_location" => {
				data.map(_.split(",")).filter(arr => parse(arr) != null)
															.map(arr => parse(arr).asInstanceOf[RZXLocationBase.Location]).toDF
			}
			case "trail" => {
				data.map(_.split(",")).filter(arr => parse(arr) != null)
															.map(arr => parse(arr).asInstanceOf[RZXTrailBase.Trail]).toDF
			}
			case "sensordoor_face" => {
				data.map(_.split(",")).filter(arr => parse(arr) != null)
															.map(arr => parse(arr).asInstanceOf[SensorFaceBase.Face]).toDF
			}
			case "sensordoor_heartbeat" => {
				data.map(_.split(",")).filter(arr => parse(arr) != null)
															.map(arr => parse(arr).asInstanceOf[SensorHeartbeatBase.HeartBeat]).toDF
			}
			case "sensordoor_idcard" => {
				data.map(_.split(",")).filter(arr => parse(arr) != null)
															.map(arr => parse(arr).asInstanceOf[SensorIdcardBase.Idcard]).toDF
			}
			case "sensordoor_idcardresult" => {
				data.map(_.split(",")).filter(arr => parse(arr) != null)
															.map(arr => parse(arr).asInstanceOf[SensorIdcardResultBase.IdcardResult]).toDF
			}
			case "ty_imsi" => {
				data.map(_.split(",")).filter(arr => parse(arr) != null)
															.map(arr => parse(arr).asInstanceOf[TYIMSIBase.IMSI]).toDF
			}
			case "ty_mac" => {
				data.map(_.split(",")).filter(arr => parse(arr) != null)
															.map(arr => parse(arr).asInstanceOf[TYMACBase.MAC]).toDF
			}
			case "ap_point" => {
				data.map(_.split(",")).filter(arr => parse(arr) != null)
															.map(arr => parse(arr).asInstanceOf[APPointBase.Point]).toDF
			}
			case "ap_minute" => {
				data.map(_.split(",")).filter(arr => parse(arr) != null)
															.map(arr => parse(arr).asInstanceOf[APMinuteBase.Minute]).toDF
			}
			case "ty_status" => {
				data.map(_.split(",")).filter(arr => parse(arr) != null)
															.map(arr => parse(arr).asInstanceOf[TYStatusBase.Status]).toDF
			}
			case "ajm_4g" => {
				data.map(_.split(",")).filter(arr => parse(arr) != null)
															.map(arr => parse(arr).asInstanceOf[AJM4GBase.A4G]).toDF
			}
			case "ajm_wifi" => {
				data.map(_.split(",")).filter(arr => parse(arr) != null)
															.map(arr => parse(arr).asInstanceOf[AJMWIFIBase.WIFI]).toDF
			}
			case "ajm_account" => {
				data.map(_.split(",")).filter(arr => parse(arr) != null)
															.map(arr => parse(arr).asInstanceOf[AJMAccountBase.Account]).toDF
			}
			case "ajm_idcard" => {
				data.map(_.split(",")).filter(arr => parse(arr) != null)
															.map(arr => parse(arr).asInstanceOf[AJMIdcardBase.Idcard]).toDF
			}
			case "ifaas_warning" => {
				data.map(_.split(",")).filter(arr => parse(arr) != null)
															.map(arr => parse(arr).asInstanceOf[IFAASWarningBase.Warning]).toDF
			}
			case "szt" => {
				data.map(_.split(",")).filter(arr => parse(arr) != null)
															.map(arr => parse(arr).asInstanceOf[SZTBase.SZT]).toDF
			}
			case _ => null
		}

	}

	// def toParquet(sqlContext:SQLContext)(topic:String)(data:RDD[String]) = {
	// 	import sqlContext.implicits._
	// 	val parse = ParseClass(topic)_
	// 	val floderandfile = getFloderAndFile
	// 	topic match {
	// 		case "rzx_device" => {
	// 			data.map(_.split(",")).filter(arr => parse(arr) != null)
	// 														.map(arr => parse(arr).asInstanceOf[RZXDeviceBase.Device])
	// 														.toDF.write.mode(SaveMode.Append).parquet("GongAn/"+topic+"/"+floderandfile._1)
	// 		}
	// 		case "rzx_feature" => {
	// 			data.map(_.split(",")).filter(arr => parse(arr) != null)
	// 														.map(arr => parse(arr).asInstanceOf[RZXFeatureBase.Feature])
	// 														.toDF.write.mode(SaveMode.Append).parquet("GongAn/"+topic+"/"+floderandfile._1)
	// 		}
	// 		case "rzx_location" => {
	// 			data.map(_.split(",")).filter(arr => parse(arr) != null)
	// 														.map(arr => parse(arr).asInstanceOf[RZXLocationBase.Location])
	// 														.toDF.write.mode(SaveMode.Append).parquet("GongAn/"+topic+"/"+floderandfile._1)
	// 		}
	// 		case "trail" => {
	// 			data.map(_.split(",")).filter(arr => parse(arr) != null)
	// 														.map(arr => parse(arr).asInstanceOf[RZXTrailBase.Trail])
	// 														.toDF.write.mode(SaveMode.Append).parquet("GongAn/"+"rzx_"+topic+"/"+floderandfile._1)
	// 		}
	// 		case "sensordoor_face" => {
	// 			data.map(_.split(",")).filter(arr => parse(arr) != null)
	// 														.map(arr => parse(arr).asInstanceOf[SensorFaceBase.Face])
	// 														.toDF.write.mode(SaveMode.Append).parquet("GongAn/"+topic+"/"+floderandfile._1)
	// 		}
	// 		case "sensordoor_heartbeat" => {
	// 			data.map(_.split(",")).filter(arr => parse(arr) != null)
	// 														.map(arr => parse(arr).asInstanceOf[SensorHeartbeatBase.HeartBeat])
	// 														.toDF.write.mode(SaveMode.Append).parquet("GongAn/"+topic+"/"+floderandfile._1)
	// 		}
	// 		case "sensordoor_idcard" => {
	// 			data.map(_.split(",")).filter(arr => parse(arr) != null)
	// 														.map(arr => parse(arr).asInstanceOf[SensorIdcardBase.Idcard])
	// 														.toDF.write.mode(SaveMode.Append).parquet("GongAn/"+topic+"/"+floderandfile._1)
	// 		}
	// 		case "sensordoor_idcardresult" => {
	// 			data.map(_.split(",")).filter(arr => parse(arr) != null)
	// 														.map(arr => parse(arr).asInstanceOf[SensorIdcardResultBase.IdcardResult])
	// 														.toDF.write.mode(SaveMode.Append).parquet("GongAn/"+topic+"/"+floderandfile._1)
	// 		}
	// 		case "ty_imsi" => {
	// 			data.map(_.split(",")).filter(arr => parse(arr) != null)
	// 														.map(arr => parse(arr).asInstanceOf[TYIMSIBase.IMSI])
	// 														.toDF.write.mode(SaveMode.Append).parquet("GongAn/"+topic+"/"+floderandfile._1)
	// 		}
	// 		case "ty_mac" => {
	// 			data.map(_.split(",")).filter(arr => parse(arr) != null)
	// 														.map(arr => parse(arr).asInstanceOf[TYMACBase.MAC])
	// 														.toDF.write.mode(SaveMode.Append).parquet("GongAn/"+topic+"/"+floderandfile._1)
	// 		}
	// 		case "ap_point" => {
	// 			data.map(_.split(",")).filter(arr => parse(arr) != null)
	// 														.map(arr => parse(arr).asInstanceOf[APPointBase.Point])
	// 														.toDF.write.mode(SaveMode.Append).parquet("GongAn/"+topic+"/"+floderandfile._1)
	// 		}
	// 		case "ap_minute" => {
	// 			data.map(_.split(",")).filter(arr => parse(arr) != null)
	// 														.map(arr => parse(arr).asInstanceOf[APMinuteBase.Minute])
	// 														.toDF.write.mode(SaveMode.Append).parquet("GongAn/"+topic+"/"+floderandfile._1)
	// 		}
	// 		case "ty_status" => {
	// 			data.map(_.split(",")).filter(arr => parse(arr) != null)
	// 														.map(arr => parse(arr).asInstanceOf[TYStatusBase.Status])
	// 														.toDF.write.mode(SaveMode.Append).parquet("GongAn/"+topic+"/"+floderandfile._1)
	// 		}
	// 		case "ajm_4g" => {
	// 			data.map(_.split(",")).filter(arr => parse(arr) != null)
	// 														.map(arr => parse(arr).asInstanceOf[AJM4GBase.A4G])
	// 														.toDF.write.mode(SaveMode.Append).parquet("GongAn/"+topic+"/"+floderandfile._1)
	// 		}
	// 		case "ajm_wifi" => {
	// 			data.map(_.split(",")).filter(arr => parse(arr) != null)
	// 														.map(arr => parse(arr).asInstanceOf[AJMWIFIBase.WIFI])
	// 														.toDF.write.mode(SaveMode.Append).parquet("GongAn/"+topic+"/"+floderandfile._1)
	// 		}
	// 		case "ajm_account" => {
	// 			data.map(_.split(",")).filter(arr => parse(arr) != null)
	// 														.map(arr => parse(arr).asInstanceOf[AJMAccountBase.Account])
	// 														.toDF.write.mode(SaveMode.Append).parquet("GongAn/"+topic+"/"+floderandfile._1)
	// 		}
	// 		case "ajm_idcard" => {
	// 			data.map(_.split(",")).filter(arr => parse(arr) != null)
	// 														.map(arr => parse(arr).asInstanceOf[AJMIdcardBase.Idcard])
	// 														.toDF.write.mode(SaveMode.Append).parquet("GongAn/"+topic+"/"+floderandfile._1)
	// 		}
	// 		case "ifaas_warning" => {
	// 			data.map(_.split(",")).filter(arr => parse(arr) != null)
	// 														.map(arr => parse(arr).asInstanceOf[IFAASWarningBase.Warning])
	// 														.toDF.write.mode(SaveMode.Append).parquet("GongAn/"+topic+"/"+floderandfile._1)
	// 		}
	// 		case "szt" => {
	// 			data.map(_.split(",")).filter(arr => parse(arr) != null)
	// 														.map(arr => parse(arr).asInstanceOf[SZTBase.SZT])
	// 														.toDF.write.mode(SaveMode.Append).parquet("GongAn/"+topic+"/"+floderandfile._1)
	// 		}
	// 		case _ => null
	// 	}
  //
	// }

	// def toParquet2(sqlContext:SQLContext)(topic:String)(data:RDD[String]) = {
	// 	import sqlContext.implicits._
	// 	val floderandfile = getFloderAndFile
	// 	val save = (base:Core) => data.map(_.split(",")).filter(arr => base.parseClass(arr.toList) != None).map(arr => base.parseClass(arr.toList))
	// 													       .toDF.write.parquet("GongAn/"+topic+"/"+floderandfile._1+"/"+floderandfile._2)
	// 	topic match {
	// 		case "rzx_feature" => save(RZXFeatureBase)
	// 		case "sensordoor_idcard" => save(SensorIdcardBase)
	// 		case "ty_imsi" => save(TYIMSIBase)
	// 		case "ty_mac" => save(TYMACBase)
	// 		case "ap_point" => save(APBase)
	// 		case _ => null
	// 	}
	//
	// }

	// def saveDF[T <: Core](sqlContext:SQLContext)(base :T,topic:String,data:RDD[String]) = {
	//
	// 	data.map(_.split(",")).map(arr => base.parseClass(arr.toList))
	// 												.toDF.write.parquet("GongAn/"+topic+"/"+floderandfile._1+"/"+floderandfile._2)
	// }

	def getFloderAndFile() = {
		val format = new java.text.SimpleDateFormat("yyyyMMdd")
		lazy val now = new java.util.Date()
		(format.format(now),now.getTime.toString)
	}

	def ParseClass(topic:String)(arr:Array[String]):Any = {
		val data = arr.toList
		val any = topic match {
			case "ap_point" => APPointBase.parseClass(data)
			case "ap_minute" => APMinuteBase.parseClass(data)
			case "rzx_device" => RZXDeviceBase.parseClass(data)
			case "rzx_location" => RZXLocationBase.parseClass(data)
			case "rzx_feature" => RZXFeatureBase.parseClass(data)
			case "trail" => RZXTrailBase.parseClass(data)
			case "sensordoor_face" => SensorFaceBase.parseClass(data)
			case "sensordoor_heartbeat" => SensorHeartbeatBase.parseClass(data)
			case "sensordoor_idcard" => SensorIdcardBase.parseClass(data)
			case "sensordoor_idcardresult" => SensorIdcardResultBase.parseClass(data)
			case "ty_imsi" => TYIMSIBase.parseClass(data)
			case "ty_mac" => TYMACBase.parseClass(data)
			case "ty_status" => TYStatusBase.parseClass(data)
			case "ajm_4g" => AJM4GBase.parseClass(data)
			case "ajm_wifi" => AJMWIFIBase.parseClass(data)
			case "ajm_account" => AJMAccountBase.parseClass(data)
			case "ajm_idcard" => AJMIdcardBase.parseClass(data)
			case "ifaas_warning" => IFAASWarningBase.parseClass(data)
			case "szt" => SZTBase.parseClass(data)
			case _ => return null
		}
		any match {
			case Some(c) => c
			case _ => return null
		}
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
