package com.qf.project1

import com.qf.utils.{SchemaUtil, StringTypes}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 进行数据格式转换
  */
object LogParquet_1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化级别
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 设置压缩方式
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    // 处理数据
    val lines = sc.textFile("E:\\textLog.txt")
    // 设置过滤条件和切分条件 内部如果切割条件相连过多，那么 需要设置切割处理条件
    val rowRDD = lines.map(t=>t.split(",",-1)).filter(t=>t.length>=85).map(arr=>{
      Row(
        arr(0),
        StringTypes.toInt(arr(1)),
        StringTypes.toInt(arr(2)),
        StringTypes.toInt(arr(3)),
        StringTypes.toInt(arr(4)),
        arr(5),
        arr(6),
        StringTypes.toInt(arr(7)),
        StringTypes.toInt(arr(8)),
        StringTypes.toDouble(arr(9)),
        StringTypes.toDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        StringTypes.toInt(arr(17)),
        arr(18),
        arr(19),
        StringTypes.toInt(arr(20)),
        StringTypes.toInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        StringTypes.toInt(arr(26)),
        arr(27),
        StringTypes.toInt(arr(28)),
        arr(29),
        StringTypes.toInt(arr(30)),
        StringTypes.toInt(arr(31)),
        StringTypes.toInt(arr(32)),
        arr(33),
        StringTypes.toInt(arr(34)),
        StringTypes.toInt(arr(35)),
        StringTypes.toInt(arr(36)),
        arr(37),
        StringTypes.toInt(arr(38)),
        StringTypes.toInt(arr(39)),
        StringTypes.toDouble(arr(40)),
        StringTypes.toDouble(arr(41)),
        StringTypes.toInt(arr(42)),
        arr(43),
        StringTypes.toDouble(arr(44)),
        StringTypes.toDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        StringTypes.toInt(arr(57)),
        StringTypes.toDouble(arr(58)),
        StringTypes.toInt(arr(59)),
        StringTypes.toInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        StringTypes.toInt(arr(73)),
        StringTypes.toDouble(arr(74)),
        StringTypes.toDouble(arr(75)),
        StringTypes.toDouble(arr(76)),
        StringTypes.toDouble(arr(77)),
        StringTypes.toDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        StringTypes.toInt(arr(84))
      )
    })
    val df = sQLContext.createDataFrame(rowRDD,SchemaUtil.structType)
    df.write.parquet("out")
    // 关闭
    sc.stop()
  }
}
