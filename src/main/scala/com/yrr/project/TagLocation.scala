package com.yrr.project

import com.qf.utils.TagTrait
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


object TagLocation extends TagTrait {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val province = row.getAs[String]("provincename")
    if (StringUtils.isNotBlank(province)){
      list:+=("ZP:"+province,1)
    }
    val city = row.getAs[String]("cityname")
    if (StringUtils.isNotBlank(city)){
      list:+=("ZC:"+city,1)
    }
    list
  }
}
