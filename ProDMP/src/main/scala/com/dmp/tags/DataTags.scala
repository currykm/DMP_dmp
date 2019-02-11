package com.dmp.tags

import com.dmp.`trait`.ProcessData
import com.dmp.tags.mkTags.{Tags_APP, Tags_adType, Tags_channel}
import com.dmp.tools.{ConstantsSQL, DateUtils, GlobalConfigUtils}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.kudu.spark.kudu._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}

object DataTags extends ProcessData{
  //todo:提前过滤掉不符合规范的数据集
  val KUDU_MASTER=GlobalConfigUtils.kuduMaster
  val SOURCE_TABLE=GlobalConfigUtils.ODS_PREFIX+DateUtils.NowDate()
  val kuduOptions:Map[String,String]=Map(
    "kudu.master"->KUDU_MASTER,
    "kudu.table"->SOURCE_TABLE
  )
  override def process(sQLContext: SQLContext, sparkContext: SparkContext, kuduContext: KuduContext): Unit ={
    //加载字典配置文件
    val appIDName: RDD[String] = sparkContext.textFile(GlobalConfigUtils.appID_name)
    val devicedic: RDD[String] = sparkContext.textFile(GlobalConfigUtils.devicedic)
    //处理字典文件
    val app = appIDName.map{
      var map = Map[String,String]()
      line =>
        val arr = line.split("##")
        map +=(arr(0)->arr(1))
        map
    }.collect().flatten.toMap

    val device = devicedic.map{
      var map = Map[String,String]()
      line =>
        val arr = line.split("##")
        map +=(arr(0)->arr(1))
        map
    }.collect().flatten.toMap
    //3.广播字典文件
    val Bapp: Broadcast[Map[String, String]] = sparkContext.broadcast(app)
    val Bdevice: Broadcast[Map[String, String]] = sparkContext.broadcast(device)
    val data: DataFrame = sQLContext.read.options(kuduOptions).kudu
    val ods: Dataset[Row] = data.where(ConstantsSQL.non_empty_UID)
    
    
    //生成标签
    val odsrdd: RDD[Row] = ods.rdd
    //line 是row类型
    odsrdd.map{
      line =>
        //广告位标签
        val advType =Tags_adType.makeTags(line)
        //APP标签
        val appname = Tags_APP.makeTags(line,Bapp)
        //渠道
        val channel = Tags_channel.makeTags(line)


    }
  }
}
