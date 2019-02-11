package com.dmp.ETL

import com.dmp.`trait`.ProcessData
import com.dmp.tools._
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

//todo:广告投放地域分布情况
object RegionalAnalysis extends ProcessData {
  val KUDU_MASTER=GlobalConfigUtils.kuduMaster
  val SOURCE_TABLE=GlobalConfigUtils.ODS_PREFIX+DateUtils.NowDate()

  val SINK_TABLE=GlobalConfigUtils.AdvertisingAnalytis
  //将其整合
  val kuduOptions:Map[String,String] = Map(
    "kudu.table" ->SOURCE_TABLE,
    "kudu.master"-> KUDU_MASTER
  )

  override def process(sQLContext: SQLContext, sparkContext: SparkContext, kuduContext: KuduContext): Unit = {
    //1.查询地域分布情况
    val ods = sQLContext.read.options(kuduOptions).kudu
    ods.registerTempTable("ods")
    val regionAnalysis: DataFrame = sQLContext.sql(ConstantsSQL.advertisingRegion)
    regionAnalysis.registerTempTable("regionTemp")
    val result: DataFrame = sQLContext.sql(ConstantsSQL.adRegionAnalysis)
    result.show(10)
    //todo:创建表
    //val schema: Schema = ConstantsSchema.Regional_Analysis
   // val partitionID = "provincename"
    //将数据落地到kudu
    //DBUtils.process(kuduContext,result,SINK_TABLE,KUDU_MASTER,schema,partitionID)




    //2.创建表
    //3.将数据保存到kudu中
  }
}
