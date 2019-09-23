package com.yrr.project

import com.qf.utils.TagTrait
import org.apache.spark.sql.Row

object TagNet extends TagTrait {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val net = row.getAs[String]("networkmannername")
    net match{
      case "WIFI" => list:+=("D00020001",1)
      case "4G" => list:+=("D00020002",1)
      case "3G" => list:+=("D00020003",1)
      case "2G" => list:+=("D00020004",1)
      case _  => list:+=("D00020005",1)
    }
    list
  }
}
