import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import scala.util.Try

import com.sibat.gongan.imp._
import com.sibat.gongan.base._


object Main  extends IPropertiesTrait{

	def sparkContextInit(url:String)(appName:String)(confset:Map[String,String]):SparkContext = {
		val conf = new SparkConf().setAppName(appName).setMaster(url)
		for (cs <- confset) conf.set(cs._1,cs._2)
		new SparkContext(conf)
	}

	def main(args: Array[String]): Unit = {


		//指定ID
		val sc = sparkContextInit(SPARKMASTER)(APPNAME)(
			Map(
					"es.nodes" -> ESNODES,
					"es.port" -> ESPORT
					)
		)

		sc.addFile("/home/hadoop/Gongan/Combine/common.properties")

		val sqlContext:SQLContext = new SQLContext(sc)

		val topics = INPUTTOPICS.split(",")
		// sc.setLogLevel("WARN")
		val trailDFs = List("rzx_feature","sensordoor_idcard","ap_point","ty_imsi",
												"ty_mac","ajm_4g","ajm_wifi","ajm_account",
												"ajm_idcard","szt")



		// PersonalBase.getDS(sqlContext).write.parquet("personal")
		// val personal = sqlContext.read.parquet("PersonalMacImsiSzt")
		// EsSpark.saveToEs(PersonalBase.getDS(sqlContext).rdd, "personal/docs",Map("es.mapping.id" -> "idno"))
		// EsSpark.saveToEs(personal.rdd.map(_.mkString(",")).map(PersonalBase.testParse(_)), "personal/docs",Map("es.mapping.id" -> "idno"))
		// val personal = sqlContext.read.format("org.elasticsearch.spark.sql").load("personal/docs")

		// personal.write.mode(SaveMode.Overwrite).parquet("personals")


		// val df1 = sqlContext.read.parquet("GongAn/rzx_feature/20171110/")
		// val df2 = RZXFeatureBase.parseCoreClass(df1)
		// val df3 = RZXFeatureBase.parseCore2Class(df1)

		//动态写入不同的块
		// val game = Map("media_type"->"game","title" -> "FF VI","year" -> "1994")
		// val book = Map("media_type" -> "book","title" -> "Harry Potter","year" -> "2010")
		// val cd = Map("media_type" -> "music","title" -> "Surfing With The Alien")
		//
		// sc.makeRDD(Seq(game, book, cd)).saveToEs("my-collection/{media_type}")

		//指定ID
		// val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
		// val muc = Map("iata" -> "MUC", "name" -> "Munich")
		// val sfo = Map("iata" -> "SFO", "name" -> "San Fran")
		//
		// val airportsRDD = sc.makeRDD(Seq((1, otp), (2, muc), (3, sfo)))
		// airportsRDD.saveToEsWithMeta("airports/2015")

		// 时空碰撞
		

		sc.stop()
	}

	def getYesterDay() = {
		val timeformat = new java.text.SimpleDateFormat("yyyyMMdd")
		val cal = java.util.Calendar.getInstance()
		val now = new java.util.Date()
		cal.setTime(now)
		cal.add(java.util.Calendar.DAY_OF_MONTH,-1)
		val time2 = cal.getTime
		timeformat.format(cal.getTime)
	}

}
