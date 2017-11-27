package com.sibat.gongan.imp


trait StatusTrait {
  //未查找到
  val NOTFOUND = "1001";

  //普通人
  val ORIDINARY = "1002";

  //重点人员
  val IMPORTANT = "1003";

  //数据异常
  val PARSEERROR = "1004";

  val ESIMPORTANT = "important"
}
