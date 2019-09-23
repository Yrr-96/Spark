package com.yrr.project

import com.qf.utils.TagTrait
import org.apache.spark.sql.Row

object TagIsp extends TagTrait {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val isp = row.getAs[String]("ispname")
    isp match {
      case "移动" => list:+=("D00030001",1)
      case "电信" => list:+=("D00030003",1)
      case "联通" => list:+=("D00030002",1)
      case _ => list:+=("D00030004",1)
    }
    list
  }
}
