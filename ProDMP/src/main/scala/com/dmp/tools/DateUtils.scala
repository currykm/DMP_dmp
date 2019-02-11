package com.dmp.tools

import java.util.Date

import org.apache.commons.lang.time.FastDateFormat
import org.apache.commons.lang3.StringUtils

//todo:和dataUtils形同，获取当前时间
object DateUtils {
  //定义一个方法
  /**
    * 获取当前的时间
    * */
  def NowDate():String = {
   val now =new Date()
  //定义实例化格式
   val fastDateFormat: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
   val dateFormat: String = fastDateFormat.format(now)
    val date: String = dfmat(dateFormat).getOrElse("sorry my boot")
      date

 }
  //进行格式化日期
  def dfmat(dateFormat:String):Option[String]={
    try{
      if(StringUtils.isNotEmpty(dateFormat)){
        val fields: Array[String] = dateFormat.split(" ")
        if(fields.length>1){
          Some(fields(0).replace("-",""))
        }else{
          None
        }
      }else{
        None
      }

    }catch{
      case  _:Exception =>None
    }
}


}

