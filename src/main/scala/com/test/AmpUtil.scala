package com.test
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.test.TypeTag
import com.test.AmpUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object AmpUtil {

  def getPOIS(row: Row): List[(String, String)] = {
    var list = List[(String, String)]()
    val jsonStr: String = row.getString(0)
    val json: JSONObject = JSON.parseObject(jsonStr)
    if (json == null) return list
    val status: Integer = json.getInteger("status")

    if (status != null & status == 1) {
      val regeocode: JSONObject = json.getJSONObject("regeocode")
      if (regeocode == null) return list
      val pois: JSONArray = regeocode.getJSONArray("pois")
      if (pois == null) return list

      for (elem <- pois.toArray) {
        val obj: JSONObject = elem.asInstanceOf[JSONObject]
        val businessArea: String = obj.getString("businessarea")
        val typeStr: String = obj.getString("type")
        list :+= (businessArea, typeStr)
      }
    }

    list
  }
}
