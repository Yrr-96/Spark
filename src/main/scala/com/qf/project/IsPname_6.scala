package com.qf.project

import com.qf.utils.RptUtil
import org.apache.spark.sql.SparkSession

object IsPname_6 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(getClass.getName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val df = spark.read.parquet("F:\\Git\\gp1923_test\\Spark\\out")
    df.rdd.map(row=>{
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adordeerid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

      val rptList = RptUtil.ReqPt(requestmode,processnode)

      val clickList = RptUtil.clickPt(requestmode,iseffective)

      val adList = RptUtil.adPt(iseffective,isbilling,isbid,iswin,adordeerid,winprice,adpayment)

      val allList:List[Double] = rptList ++ clickList ++ adList
      (RptUtil.isPname(row.getAs[String]("ispname")),allList)
    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>{
        t._1+t._2
      })
    })
      .saveAsTextFile("out5")
  }
}
