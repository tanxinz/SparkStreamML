package com.sibat.gongan.base

import com.sibat.gongan.util.EZJSONParser
import com.sibat.gongan.imp.{Core,ESQueryTrait,IPropertiesTrait}
import java.util.Properties
import org.apache.spark.sql.SQLContext

object PersonalBase extends Core with ESQueryTrait with IPropertiesTrait {

  case class Personal(idno:java.lang.String,name :String,former_name :String,foreign_name :String,
                      sex:String,birthday :String,nationality :String,national :String,
                      education :String,marital_status :String,residence_addr :String,create_time :String,
                      household_flag :String,nowlive :String,id_number_18 :String,deptid:String,
                      zdrytype :String,zdrystate :String,globalmanage :String,nickname :String,
                      birthplace :String,currentwork :String,convictions :String,maincontrol :String,
                      deptuser :String,deptusername :String,deptname :String,rybh :String,
                      zjzl :String,zhcs :String,ch :String,sf :String,
                      wetherxd :String,lx :String,sg :String,remark :String,
                      gjryzt :String,addperson :String,sztkh :String,phone :String,
                      qq :String,email :String,addpersonid :String,sjly :String,
                      update_time :String,check_status :String,important:String
                    )

  case class TestPersonal(idno:java.lang.String,name :String,former_name :String,foreign_name :String,
                                        sex:String,birthday :String,nationality :String,national :String,
                                        education :String,marital_status :String,residence_addr :String,create_time :String,
                                        household_flag :String,nowlive :String,id_number_18 :String,deptid:String,
                                        zdrytype :String,zdrystate :String,globalmanage :String,nickname :String,
                                        birthplace :String,currentwork :String,maincontrol :String,
                                        deptuser :String,deptusername :String,deptname :String,rybh :String,
                                        zjzl :String,zhcs :String,ch :String,sf :String,
                                        wetherxd :String,lx :String,sg :String,remark :String,
                                        gjryzt :String,addperson :String,sztkh :String,phone :String,
                                        qq :String,email :String,addpersonid :String,sjly :String,
                                        update_time :String,check_status :String,important:String,mac:String,imsi:String,szt:String
                                      )

  def caseNull(a:Any) = {
      if(a == null) ""
      else a.toString
  }

  def testParse(data:String) = {
    val rs = data.split(",")
    TestPersonal(rs(0),rs(1),rs(2),rs(3),rs(4),rs(5),rs(6),
                              rs(7),rs(8),rs(9),rs(10),rs(11),rs(12),rs(13),
                              rs(14),rs(15),rs(16),rs(17),rs(18),rs(19),rs(20),
                              rs(21),rs(22),rs(23),rs(24),rs(25),rs(26),rs(27),
                              rs(28),rs(29),rs(30),rs(31),rs(32),rs(33),rs(34),
                              rs(35),rs(36),rs(37),rs(38),rs(39),rs(40),
                              rs(41),rs(42),rs(43),rs(44),rs(45),rs(46),rs(47),rs(48))
  }


  def getDS(sqlContext:SQLContext) = {
    import sqlContext.implicits._
    val connectionProperties = new Properties()
		//增加数据库的用户名(user)密码(password),指定postgresql驱动(driver)
		connectionProperties.put("user",POSTGRESUSER);
		connectionProperties.put("password",POSTGRESPASSWD);
		connectionProperties.put("driver","org.postgresql.Driver")
    sqlContext.read.jdbc("jdbc:postgresql://"+POSTGRESIP+":"+POSTGRESPORT+"/"+POSTGRESDATABASE,POSTGRESTABLE,connectionProperties).na.fill("")
                    .map(rs => {println(rs)
                      Personal(rs.getString(0),rs.getString(1),rs.getString(2),rs.getString(3),rs.getString(4),caseNull(rs.getTimestamp(5)),rs.getString(6),
                              rs.getString(7),rs.getString(8),rs.getString(9),rs.getString(10),caseNull(rs.getTimestamp(11)),rs.getString(12),rs.getString(13),
                              rs.getString(14),rs.getString(15),rs.getString(16),rs.getString(17),rs.getString(18),rs.getString(19),rs.getString(20),
                              rs.getString(21),rs.getString(22),rs.getString(23),rs.getString(24),rs.getString(25),rs.getString(26),rs.getString(27),
                              rs.getString(28),rs.getString(29),rs.getString(30),rs.getString(31),rs.getString(32),rs.getString(33),rs.getString(34),
                              rs.getString(35),rs.getString(36),rs.getString(37),rs.getString(38),rs.getString(39),rs.getString(40),rs.getString(41),
                              rs.getString(42),rs.getString(43),caseNull(rs.getTimestamp(44)),caseNull(rs.getDouble(45)),"1"
                            )})


  }

}
