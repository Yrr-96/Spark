package com.qf.utils

/**
  * 处理指标统计工具类
  */
object RptUtil {

  // 处理请求数
  def ReqPt(requestmode:Int,processnode:Int):List[Double]={
    if(requestmode ==1 && processnode ==1){
      List[Double](1,0,0)
    }else if(requestmode ==1 && processnode ==2){
      List[Double](1,1,0)
    }else if(requestmode ==1 && processnode ==3){
      List[Double](1,1,1)
    }else{
      List[Double](0,0,0)
    }
  }
  // 处理点击展示数
  def clickPt(requestmode:Int,iseffective:Int):List[Double]={
    if(requestmode ==2 && iseffective ==1){
      List[Double](1,0)
    }else if(requestmode ==3 && iseffective ==1){
      List[Double](0,1)
    }else{
      List[Double](0,0)
    }
  }
  // 处理竞价，成功数广告成品和消费
  def adPt(iseffective:Int,isbilling:Int,
           isbid:Int,iswin:Int,adordeerid:Int,winprice:Double,adpayment:Double):List[Double]={
    if(iseffective ==1 && isbilling ==1 && isbid ==1){
      if(iseffective ==1 && isbilling ==1 && iswin ==1 && adordeerid !=0){
        List[Double](1,1,winprice/1000.0,adpayment/1000.0)
      }else{
        List[Double](1,0,0,0)
      }
    }else{
      List[Double](0,0,0,0)
    }
  }
  def isPname(ispname:String):String={
    if(ispname=="电信"||ispname=="联通"||ispname=="移动"){
      ispname
    }else{
      "其他"
    }
  }
  def netWorkMannerName(networkmannername:String):String={
    if(networkmannername=="Wifi"||networkmannername=="2G"||networkmannername=="3G"||networkmannername=="4G"){
      networkmannername
    }else{
      "其他"
    }
  }
  def deviceType(devicetype:Int):String={
    devicetype match{
      case 1 => "手机"
      case 2 => "平板"
      case _ => "其他"
    }
  }
  def client(clientId:Int):String={
    clientId match{
      case 1 => "android"
      case 2 => "ios"
      case _ => "其他"
    }
  }
}
