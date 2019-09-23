package com.qf.utils

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

object HttpUtil {
  def get(url:String):String={

    val client = HttpClients.createDefault()
    val httpGet = new HttpGet(url)
    // 发送请求
    val httpResponse = client.execute(httpGet)
    // 处理返回请求结果
    EntityUtils.toString(httpResponse.getEntity,"UTF-8")
  }
}
