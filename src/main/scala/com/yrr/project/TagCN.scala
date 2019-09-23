package com.yrr.project

import com.qf.utils.TagTrait
import org.apache.spark.sql.Row


object TagCN extends TagTrait {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //获取数据类型
    val row = args(0).asInstanceOf[Row]
    //渠道
    val adplatformproviderid = row.getAs[Int]("adplatformproviderid")
    list:+=("CN:"+adplatformproviderid,1)
    list
  }
}
